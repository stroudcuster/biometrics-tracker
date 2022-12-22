from datetime import datetime, timedelta
import logging
import logging.config
import os
import pathlib
import sys
from time import sleep
import threading
from typing import Any, Callable, Optional

import schedule

import biometrics_tracker.config.createconfig as config
import biometrics_tracker.ipc.messages as messages
import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.main.dispatcher as dispatcher
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.persistence as per
import biometrics_tracker.utilities.utilities as utilities


def run_scheduled_entry(**kwargs):
    """
    This method creates and runs an instance of biometrics_tracker.gui.ScheduledEntryWindow.  It is started as a
    separate process because the Tkinter event loop must be in the main thread.

    :param logger: a logger instance
    :type logger: logging.Logger
    :param entry: the ScheduleEntry that triggered the invocation of this method
    :type entry: biometrics_tracker.model.datapoints.ScheduleEntry
    :param log_msg: a message to be written to the logger
    :type log_msg: str
    :return: None

    """
    def parm_error(parm: str) -> str:
        return f'biometrics_tracker.main.scheduler.run_scheduled_entry: {parm} is a require parameter'

    assert 'logger' in kwargs, parm_error('logger')
    assert 'entry' in kwargs, parm_error('entry')
    assert 'log_msg' in kwargs, parm_error('log_msg')

    logger: logging.Logger = kwargs['logger']
    entry: dp.ScheduleEntry = kwargs['entry']
    log_msg: str = kwargs['log_msg']
    pid = os.fork()
    if pid == 0:
        path: pathlib.Path = pathlib.Path(utilities.whereami('biometrics_tracker'), 'main', 'main.py')
        logger.info(f'Launching {log_msg}')
        # the second 'python' arg is passed to the Python interpreter as argv[0].  The second arg
        # parm, the path to the Python script, becomes the script's argv[0]
        os.execlp('python', 'python', path.__str__(), '--scheduled-entry', str(entry.seq_nbr), entry.person_id)
        assert False, f'Error launching {path.__str__()}'
    if entry.frequency in [dp.FrequencyType.One_Time, dp.FrequencyType.Monthly]:
        return schedule.CancelJob


class Scheduler(threading.Thread):
    """
    A threaded class the used the contents of the People and ScheduleEntry database tables to present single entry
    windows for according to the schedule
    """
    def __init__(self, config_info: config.ConfigInfo, queue_mgr: queues.Queues, start_dispatcher: bool,
                 completion_replyto: Optional[Callable] = None, thread_sleep_seconds: int = 300,
                 queue_sleep_seconds: int = 10):
        threading.Thread.__init__(self, name='biometrics-scheduler')
        self.config_info: config.ConfigInfo = config_info
        self.queue_mgr: queues.Queues = queue_mgr
        self.start_dispatcher: bool = start_dispatcher
        self.completion_replyto: Callable = completion_replyto
        self.thread_sleep_seconds = thread_sleep_seconds
        self.queue_sleep_seconds = queue_sleep_seconds
        self.people_list: list[dp.Person] = []
        self.quit: bool = False
        logging.config.dictConfig(config_info.logging_config)
        self.logger = logging.getLogger('scheduled_entry')
        self.dispatcher: Optional[dispatcher.Dispatcher] = None
        if self.start_dispatcher:
            self.dispatcher = dispatcher.Dispatcher(self.logger, self.thread_sleep_seconds)

    def run(self) -> None:
        """ Override the threading.Thread run method.  Loops until boolean quit property is True, issuing a database
        request to retrieve a list of people, which chains through successive replyto callbacks to process the
        ScheduleEntry's for each person and kicks of jobs that present biometrics_tracking.gui.ScheduledEntryWindow
        instances
        
        :return: None

        :TODO This process should run once, and then (maybe) start a thread to process the schedule jobs via schedule.run_pending

        """
        self.logger.info('Scheduler')
        self.quit = False
        while not self.quit:
            try:
                self.retrieve_people()
                self.check_response_queue(block=True, caller='Scheduler.run')
                if self.completion_replyto is not None:
                    self.logger.debug('Sending Scheduler Completion Message')
                    self.queue_mgr.send_completion_msg(
                        messages.SchedulerCompletionMsg(destination=self.completion_replyto,
                                                        replyto=None, jobs=schedule.get_jobs(),
                                                        status=messages.Completion.SUCCESS))
                if self.dispatcher is not None:
                    if schedule.idle_seconds() is not None:
                        self.dispatcher.start()
                        self.logger.info('Dispatcher started.')
                    else:
                        self.dispatcher = None
                        self.logger.info('No jobs scheduled, dispatcher not started')
            except schedule.ScheduleError:
                self.queue_mgr.send_completion_msg(
                    messages.SchedulerCompletionMsg(destination=self.completion_replyto,
                                                    replyto=None, jobs=schedule.get_jobs(),
                                                    status=messages.Completion.FAILURE))
                logging.error(sys.exc_info)
                logging.error('Job Dispatcher not started due to exception in the Scheduler process.')
            finally:
                self.logger.info('Scheduler Ending.')

    def stop(self):
        self.quit = True
        self.logger.info('Initiated Scheduler End')

    def check_response_queue(self, block: bool, caller: str):
        """
        Check the database response queue for responses to requests

        :param block: should the queue check be a blocking check
        :param caller: a string name of the invoking method for inclusion in log entries
        :return: None

        """
        self.logger.debug(f'{caller} checking db response queue')
        start: datetime = datetime.now()
        msg: Optional[messages.db_resp_msg_type] = None
        howlong: timedelta = datetime.now() - start
        while not self.quit and msg is None and howlong.seconds < 120:
            msg = self.queue_mgr.check_db_resp_queue(block=block)
            if msg is not None and msg.destination is not None:
                msg.destination(msg)
                start = datetime.now()
            else:
                howlong: timedelta = datetime.now() - start
                self.logger.debug(f'{caller} db response queue timeout. Total wait time {howlong.seconds} seconds')
                sleep(self.queue_sleep_seconds)

    def retrieve_people(self):
        """
        Issues a PersonReqMsg to the database to retrieve a list of person_id's, passing control onto the
        process_people method through the replyto callback.

        :return: None

        """
        self.logger.info('Requesting People list')
        self.queue_mgr.send_db_req_msg(messages.PersonReqMsg(destination=per.DataBase,
                                                             replyto=self.process_people,
                                                             person_id=None,
                                                             operation=messages.DBOperation.RETRIEVE_SET))
        self.check_response_queue(block=True, caller='Scheduler.retrieve_people')

    def process_people(self, msg: messages.PeopleMsg):
        """
        Received a list of people and processes each entry in the list

        :param msg: a message containing a list of Person instances
        :type msg: biometrics_tracker.ipc.messages.PeopleMsg

        :return: None

        """
        self.logger.info('People list received')
        self.people_list = msg.payload
        for person_id, person_name in self.people_list:
            self.logger.info(f'Requesting Schedules for ID:{person_id} Name:{person_name}')
            self.retrieve_schedules(self.process_schedule_entries, person_id)
        jobs = schedule.get_jobs()
        self.quit = True

    def retrieve_schedules(self, replyto: Callable, person_id: str):
        """
        Issue a ScheduleEntryReqMsg to the database to retrieve the ScheduleEntry's for a person

        :param replyto: the method or function to serve as the 'replyto' callback
        :type replyto: Callable
        :param person_id: the ID of the person whose ScheduleEntry's are to be retrieved
        :type person_id: str

        :return: None

        """
        self.queue_mgr.send_db_req_msg(messages.ScheduleEntryReqMsg(destination=per.DataBase,
                                                                    replyto=replyto,
                                                                    operation=messages.DBOperation.RETRIEVE_SET,
                                                                    person_id=person_id,
                                                                    seq_nbr=0, last_triggered=None))
        self.check_response_queue(block=True, caller='Scheduler.retrieve_schedules')

    def update_last_triggered(self, entry):
        self.queue_mgr.send_db_req_msg(messages.ScheduleEntryReqMsg(destination=per.DataBase, replyto=None,
                                                                    person_id=entry.person_id,
                                                                    seq_nbr=entry.seq_nbr,
                                                                    last_triggered=datetime.now(),
                                                                    operation=messages.DBOperation.UPDATE))

    def process_schedule_entries(self, msg: messages.ScheduleEntriesMsg):
        """
        Processes the ScheduleEntry's for a single person, checking each one to see if a job should be triggered

        :param msg: a database response message containing a list of ScheduleEntry's
        :type msg: biometrics_tracker.ipc.messages.ScheduleEntriesMsg
        :return: None

        """
        def make_tags() -> (str, str,str):
            return entry.person_id, f'{entry.person_id}:{entry.seq_nbr}', dp.dptype_dp_map[entry.dp_type].label()

        def cancel_jobs_for_entry() -> None:
            schedule.clear(f'{entry.person_id}:{entry.seq_nbr}')

        if len(msg.entries) > 0:
            self.logger.info(f'{len(msg.entries)} Schedule entries received for ID: {msg.person_id}')
            now_datetime: datetime = datetime.now()
            for entry in msg.entries:
                if not entry.suspended and entry.starts_on <= now_datetime.date() <= entry.ends_on:
                    next_today: list[datetime] = entry.next_occurrence_today(now_datetime.date())
                    for dattim in next_today:
                        if dattim.time() >= now_datetime.time():
                            wt_hm_str = dattim.strftime('%H:%M')
                            wt_m_str = dattim.strftime('%M:%S')
                            log_msg: str = f'{entry.frequency.name}  {dp.dptype_dp_map[entry.dp_type].label()} '\
                                           f'Interval {entry.interval} at {dattim.strftime("%m/%d/%Y %H:%M")}'
                            kwargs: dict[str, Any] = {'logger': self.logger,
                                                      'entry': entry,
                                                      'log_msg': log_msg, }
                            match entry.frequency:
                                case dp.FrequencyType.Hourly:
                                    cancel_jobs_for_entry()
                                    try:
                                        schedule.every(entry.interval).hours.at(wt_m_str).do(run_scheduled_entry,
                                                                                             **kwargs).tag(make_tags())
                                        self.logger.info(log_msg)
                                        self.update_last_triggered(entry)
                                        break
                                    except schedule.ScheduleError:
                                        self.logger.error(sys.exc_info())
                                case dp.FrequencyType.Daily:
                                    if (entry.last_triggered is not None and
                                            entry.last_triggered.date() != now_datetime.date()) or \
                                            entry.last_triggered is None:
                                        cancel_jobs_for_entry()
                                        try:
                                            schedule.every(entry.interval) \
                                                .days.at(wt_hm_str).do(run_scheduled_entry, **kwargs).tag(make_tags())
                                            self.logger.info(log_msg)
                                            self.update_last_triggered(entry)
                                        except schedule.ScheduleError:
                                            self.logger.error(sys.exc_info())
                                    else:
                                        self.logger.debug(f'{log_msg} not scheduled.  Last trigger '
                                                          f'{entry.last_triggered.strftime("%m/%d/%Y %H:%M:%S:%f")}')

                                case dp.FrequencyType.Weekly:
                                    self.logger.info(f'{log_msg} scheduled.')
                                    cancel_jobs_for_entry()
                                    for day in entry.weekdays:
                                        kwargs['log_msg'] = f'{log_msg} scheduled on {day.name}'
                                        self.logger.info(kwargs['log_msg'])
                                        try:
                                            match day:
                                                case dp.WeekDay.Monday:
                                                    schedule.every(entry.interval)\
                                                        .monday.at(wt_hm_str)\
                                                        .do(run_scheduled_entry, **kwargs).tag(make_tags())
                                                case dp.WeekDay.Tuesday:
                                                    schedule.every(entry.interval)\
                                                        .tuesday.at(wt_hm_str)\
                                                        .do(run_scheduled_entry, **kwargs).tag(make_tags())
                                                case dp.WeekDay.Wednesday:
                                                    schedule.every(entry.interval)\
                                                        .wednesday.at(wt_hm_str)\
                                                        .do(run_scheduled_entry, **kwargs).tag(make_tags())
                                                case dp.WeekDay.Thursday:
                                                    schedule.every(entry.interval)\
                                                        .thursday.at(wt_hm_str)\
                                                        .do(run_scheduled_entry, **kwargs).tag(make_tags())
                                                case dp.WeekDay.Friday:
                                                    schedule.every(entry.interval)\
                                                        .friday.at(wt_hm_str)\
                                                        .do(run_scheduled_entry, **kwargs).tag(make_tags())
                                                case dp.WeekDay.Saturday:
                                                    schedule.every(entry.interval)\
                                                        .saturday.at(wt_hm_str)\
                                                        .do(run_scheduled_entry, **kwargs).tag(make_tags())
                                                case dp.WeekDay.Sunday:
                                                    schedule.every(entry.interval)\
                                                        .sunday.at(wt_hm_str)\
                                                        .do(run_scheduled_entry, **kwargs).tag(make_tags())
                                            self.update_last_triggered(entry)
                                        except schedule.ScheduleError:
                                            self.logger.error(sys.exc_info())
                                case dp.FrequencyType.Monthly:
                                    cancel_jobs_for_entry()
                                    try:
                                        schedule.every().day.at(wt_hm_str)\
                                            .do(run_scheduled_entry,kwargs).tag(make_tags())
                                        self.logger.info(log_msg)
                                        self.update_last_triggered(entry)
                                    except schedule.ScheduleError:
                                        self.logger.error(sys.exc_info())
                                case dp.FrequencyType.One_Time:
                                    if (entry.last_triggered is not None and
                                            entry.last_triggered.date() != now_datetime.date()) or \
                                            entry.last_triggered is None:
                                        cancel_jobs_for_entry()
                                        try:
                                            schedule.every().day.at(wt_hm_str)\
                                                .do(run_scheduled_entry, kwargs).tag(make_tags())
                                        except schedule.ScheduleError:
                                            self.logger.error(sys.exc_info())

                                    else:
                                        self.logger.debug(f'{log_msg} not scheduled.  Last trigger '
                                                          f'{entry.last_triggered.strftime("%m/%d/%Y %H:%M:%S:%f")}')



