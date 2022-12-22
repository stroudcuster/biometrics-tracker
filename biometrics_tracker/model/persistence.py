from datetime import date, datetime
import json
import pathlib
import sqlite3 as sql
import threading
import time
from typing import Optional, Union

import biometrics_tracker.ipc.messages as messages
import biometrics_tracker.ipc.queue_manager as queue_manager
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.json_handler as jh
import biometrics_tracker.model.uoms as uoms
import biometrics_tracker.utilities.utilities as utilities

CREATE_STMTS = ['CREATE TABLE IF NOT EXISTS PEOPLE (ID PRIMARY KEY, NAME, DOB DATE);',
    'CREATE TABLE IF NOT EXISTS DATAPOINTS (PERSON_ID NOT NULL, TIMESTAMP DATETIME, ' +
    'TYPE INT, NOTE CHAR, DATA CHAR, FOREIGN KEY (PERSON_ID) REFERENCES PEOPLE(ID));',
    'CREATE TABLE IF NOT EXISTS SCHEDULE (PERSON_ID CHAR NOT NULL, SEQ_NBR NOT NULL, DP_TYPE CHAR,' +
    'FREQUENCY CHAR NOT NULL, STARTS_ON DATETIME, ENDS_ON DATETIME, NOTE CHAR, WEEKDAYS CHAR, DAYS_OF_MONTH CHAR, ' +
    'INTERVAL INT, WHEN_TIME TIME, SUSPENDED INT, LAST_TRIGGERED DATETIME, FOREIGN KEY(PERSON_ID) REFERENCES PEOPLE(ID));',
    'CREATE TABLE IF NOT EXISTS TRACK_CFG (PERSON_ID NOT NULL, DP_TYPE NOT NULL, UOM, ' +
    'FOREIGN KEY(PERSON_ID) REFERENCES PEOPLE(ID));',
    'CREATE INDEX IF NOT EXISTS DP_IDX ON DATAPOINTS (PERSON_ID, TIMESTAMP, TYPE);',
    'CREATE INDEX IF NOT EXISTS TRACK_CFG_IDX ON TRACK_CFG (PERSON_ID, DP_TYPE);',
    'CREATE INDEX IF NOT EXISTS SCHEDULE_IDX ON SCHEDULE (PERSON_ID, SEQ_NBR);']

DROP_STMTS = ['DROP INDEX IF EXISTS TRACK_CFG_IDX;', 'DROP INDEX IF EXISTS SCHEDULE_IDX;',
              'DROP INDEX IF EXISTS DP_IDX;', 'DROP TABLE IF EXISTS DATAPOINTS;', 'DROP TABLE IF EXISTS TRACK_CFG;',
              'DROP TABLE IF EXISTS SCHEDULE', 'DROP TABLE IF EXISTS PEOPLE;']

INSERT_PERSON_STMT = 'INSERT INTO PEOPLE (ID, NAME, DOB) VALUES(?, ?, ?);'

UPDATE_PERSON_STMT = 'UPDATE PEOPLE SET NAME = ?, DOB = ? WHERE ID = ?;'

RETRIEVE_PERSON_STMT = 'SELECT NAME, DOB FROM PEOPLE WHERE ID = ?'

DELETE_PERSON_STMT = 'DELETE FROM PEOPLE WHERE ID = ?;'

INSERT_TRACK_CFG_STMT = 'INSERT INTO TRACK_CFG (PERSON_ID, DP_TYPE, UOM) ' \
    'VALUES(?, ?, ?);'

DELETE_TRACK_CFG_FOR_PERSON_STMT = 'DELETE FROM TRACK_CFG WHERE PERSON_ID = ?;'

RETRIEVE_TRACK_CFG_FOR_PERSON_STMT = 'SELECT DP_TYPE, UOM FROM TRACK_CFG WHERE PERSON_ID = ?;'

INSERT_SCHEDULE_STMT = 'INSERT INTO SCHEDULE (PERSON_ID, SEQ_NBR, DP_TYPE, FREQUENCY, STARTS_ON, ' \
                       'ENDS_ON, NOTE, WEEKDAYS, DAYS_OF_MONTH, INTERVAL, WHEN_TIME, SUSPENDED, LAST_TRIGGERED) ' \
                       'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'

UPDATE_SCHEDULE_STMT = 'UPDATE SCHEDULE SET DP_TYPE = ?, FREQUENCY = ?, STARTS_ON = ?, ENDS_ON = ?, NOTE = ?, '\
                       'WEEKDAYS = ?, DAYS_OF_MONTH = ?, INTERVAL = ?, WHEN_TIME = ?, SUSPENDED = ? ' \
                       'WHERE PERSON_ID = ? AND SEQ_NBR = ?;'

UPDATE_SCHEDULE_LAST_TRIGGERED_STMT = 'UPDATE SCHEDULE SET LAST_TRIGGERED = ? WHERE PERSON_ID = ? AND SEQ_NBR = ?;'

DELETE_SCHEDULE_FOR_PERSON_SEQ_STMT = 'DELETE FROM SCHEDULE WHERE PERSON_ID = ? AND SEQ_NBR = ?;'

DELETE_SCHEDULES_FOR_PERSON_STMT = 'DELETE FROM SCHEDULE WHERE PERSON_ID = ?;'

RETRIEVE_SCHED_NEXT_SEQ_NBR_STMT = 'SELECT MAX(SEQ_NBR) + 1 FROM SCHEDULE WHERE PERSON_ID = ?;'

RETRIEVE_SCHEDULES_FOR_PERSON_STMT = 'SELECT SEQ_NBR, DP_TYPE, FREQUENCY, STARTS_ON, ENDS_ON, NOTE, WEEKDAYS, DAYS_OF_MONTH, ' \
    'INTERVAL, WHEN_TIME, SUSPENDED, LAST_TRIGGERED FROM SCHEDULE WHERE PERSON_ID = ?;'

RETRIEVE_SCHEDULE_FOR_PERSON_SEQ_STMT = 'SELECT SEQ_NBR, DP_TYPE, FREQUENCY, STARTS_ON, ENDS_ON, NOTE, WEEKDAYS, DAYS_OF_MONTH, ' \
    'INTERVAL, WHEN_TIME, SUSPENDED, LAST_TRIGGERED FROM SCHEDULE WHERE PERSON_ID = ? AND SEQ_NBR = ?;'

INSERT_DATAPOINT_STMT = 'INSERT INTO DATAPOINTS (PERSON_ID, TIMESTAMP, NOTE, DATA, TYPE)' \
                        'VALUES (?, ?, ?, ?, ?);'

UPDATE_DATAPOINT_STMT = 'UPDATE DATAPOINTS SET TIMESTAMP = ?, NOTE = ?, DATA = ? WHERE PERSON_ID = ? AND ' \
                        'TIMESTAMP = ? AND TYPE = ?;'

DELETE_DP_FOR_PERSON_STMT = 'DELETE FROM DATAPOINTS WHERE PERSON_ID = ?'

DELETE_DP_FOR_PER_DATETIME = 'DELETE FROM DATAPOINTS WHERE PERSON_ID = ? AND TIMESTAMP = ?'

DELETE_DP_FOR_PER_DT_TYPE = 'DELETE FROM DATAPOINTS WHERE PERSON_ID = ? AND TIMESTAMP = ? AND TYPE = ?'

RETRIEVE_DP_FOR_PERSON_STMT = 'SELECT PERSON_ID, TIMESTAMP, TYPE, NOTE, DATA FROM DATAPOINTS WHERE PERSON_ID = ? AND ' \
                              'TIMESTAMP >= ? AND TIMESTAMP <= ?;'

RETRIEVE_DP_STMT = 'SELECT PERSON_ID, TIMESTAMP, TYPE, NOTE, DATA FROM DATAPOINTS '

RETRIEVE_PEOPLE_STMT = 'SELECT ID, NAME FROM PEOPLE;'


class DataBase(threading.Thread):
    """
    Establishes a connection to the sqlite3 database and provides methods that insert/update the variable tables.
    The database contains the following tables:
    - PEOPLE - one row for each person whose biometrics are tracked by the application
    - TRACK_CFG - one row for each metric tracked for each person.  This table has a foreign key relationship with
    the PEOPLE table.
    - DATAPOINTS - one row for each instance of a recorded metric for each person.  This table has a foreign key
    relationship with the PEOPLE table
    - SCHEDULE - one row for each schedule entry for the scheduled collection of biometrics data.  This table has a
    foreign key relationship with the PEOPLE table

    """
    def __init__(self, filename: str, queues: queue_manager.Queues, block_req_queue: bool):
        """

        Create an instance of DataBase

        :param filename: the filename of the SQLite3 database
        :type filenme: str
        :param queues: a container for the IPC queues used by the application
        :type queues: biometrics_tracker.ipc.queues.Queues
        :param block_req_queue: should the database request queue fetch block
        :type block_req_queue: bool

        """
        threading.Thread.__init__(self)
        self.filename = filename
        if not pathlib.Path(filename).exists():
            self.conn = sql.connect(self.filename)
            self.create_db()
            self.conn.close()
        self.queues = queues
        self.block_req_queue = block_req_queue
        self.closed = False

    def run(self):
        """
        This method is invoked when the DataBase thread is started

        :return: None

        """
        self.conn = sql.connect(self.filename)
        while not self.closed:
            msg: messages.db_req_msg_type = self.queues.check_db_req_queue(block=self.block_req_queue)
            match msg.__class__:
                case messages.PersonMsg:
                    match msg.operation:
                        case messages.DBOperation.INSERT:
                            self.insert_person(msg.payload)
                        case messages.DBOperation.UPDATE:
                            self.update_person(msg.payload)
                    people = self.retrieve_people()
                    resp_msg: messages.PeopleMsg = messages.PeopleMsg(destination=msg.replyto, replyto=None,
                                                                      payload=people)
                    self.queues.send_db_resp_msg(resp_msg)
                case messages.DataPointMsg:
                    resp_msg: messages.DataPointRespMsg = self.handle_dp_req(msg)
                    if resp_msg is not None:
                        self.queues.send_db_resp_msg(resp_msg)
                case messages.PersonReqMsg:
                    resp_msg: messages.db_resp_msg_type = self.handle_person_req(msg)
                    if resp_msg is not None:
                        self.queues.send_db_resp_msg(resp_msg)
                case messages.ScheduleEntryMsg:
                    match msg.operation:
                        case messages.DBOperation.INSERT:
                            self.insert_schedule_entry(msg.payload)
                        case messages.DBOperation.UPDATE:
                            self.update_schedule_entry(msg.payload)
                case messages.TrackingConfigReqMsg:
                    self.handle_track_cfg_req(msg)
                case messages.ScheduleEntryReqMsg:
                    resp_msg: messages.ScheduleEntriesMsg = self.handle_schedule_req(msg)
                    if resp_msg is not None:
                        self.queues.send_db_resp_msg(resp_msg)
                case messages.DataPointReqMsg:
                    resp_msg: Optional[messages.DataPointRespMsg] = self.handle_dp_req(msg)
                    if resp_msg is not None:
                        self.queues.send_db_resp_msg(resp_msg)
                case messages.CreateDataBaseReqMsg:
                    self.create_db()
                    self.queues.send_completion_msg(messages.CompletionMsg(destination=msg.replyto, replyto=None,
                                                                           status=messages.Completion.SUCCESS))
                case messages.CloseDataBaseReqMsg:
                    self.close_db()
                    self.queues.send_completion_msg(messages.CompletionMsg(destination=msg.replyto, replyto=None,
                                                                           status=messages.Completion.DB_CLOSED))
                    return
            if not self.block_req_queue:
                time.sleep(.250)

    def close_db(self) -> None:
        """
        Close the database connection and set the closed boolean to True, when will cause the run method to exit

        :return: None

        """
        self.conn.close()
        self.closed = True

    def handle_person_req(self, msg: messages.PersonReqMsg) -> Optional[Union[messages.PersonMsg,
                                                                        messages.PeopleMsg]]:
        """
        Handle PersonReqMsg tasks - retrieve a list of Person instances or a single instance

        :param msg: the message containing the request info
        :type msg: biometrics_tracker.ipc.messages.PersonReqMsg
        :return: if appropriate, returns a response message containing the requested data
        :rtype: Optional[Union[biometrics_tracker.ipc.messages.PersonMsg,biometrics_tracker.ipc.messages.PeopleMsg]]:

        """
        resp_msg: Optional[Union[messages.PersonMsg, messages.PeopleMsg]] = None
        if msg.person_id is None:
            people_list = self.retrieve_people()
            resp_msg = messages.PeopleMsg(destination=msg.replyto, replyto=None, payload=people_list)
        else:
            person: dp.Person = self.retrieve_person(msg.person_id)
            resp_msg = messages.PersonMsg(destination=msg.replyto, replyto=None, payload=person,
                                          operation=messages.DBOperation.RETRIEVE_SINGLE)
        return resp_msg

    def handle_track_cfg_req(self, msg: Union[messages.TrackingConfigReqMsg, messages.TrackingConfigMsg]) -> None:
        """
        Handle TrackConfig requests - deletes and updates

        :param msg: a message containing the request info
        :type msg:Union[biometrics_tracker.ipc.messages.TrackingConfigReqMsg, biometrics_tracker.ipc.messages.TrackingConfigMsg]
        :return: None

        """
        match msg.__class__:
            case messages.TrackingConfigReqMsg:
                self.delete_tracking_cfg_for_person(msg.person_id)
            case messages.TrackingConfigMsg:
                self.upsert_tracking_cfg(msg.person_id, msg.payload)

    def handle_schedule_req(self, msg: Union[messages.ScheduleEntryReqMsg,
                                             messages.ScheduleEntryMsg]) -> messages.ScheduleEntriesMsg:
        """
        Handle ScheduleEntry requests - deletes retrievals and upates
        :param msg:
        :return: a message containing the response
        :rtype: biometrics_tracker.ipc.messages.ScheduleEntriesMsg

        """
        resp_msg: Optional[messages.ScheduleEntriesMsg] = None
        match msg.__class__:
            case messages.ScheduleEntryReqMsg:
                match msg.operation:
                    case messages.DBOperation.DELETE_SINGLE:
                        self.delete_schedule_for_person_seq(msg.person_id, msg.seq_nbr)
                        resp_msg = messages.ScheduleEntriesMsg(destination=msg.replyto, replyto=None,
                                                               person_id=msg.person_id, entries=[])
                    case messages.DBOperation.DELETE_SET:
                        self.delete_schedules_for_person(msg.person_id)
                        resp_msg = messages.ScheduleEntriesMsg(destination=msg.replyto, replyto=None,
                                                               person_id=msg.person_id, entries=[])
                    case messages.DBOperation.RETRIEVE_SINGLE:
                        entry: dp.ScheduleEntry = self.retrieve_schedule_for_person_seq(msg.person_id, msg.seq_nbr)
                        resp_msg = messages.ScheduleEntriesMsg(destination=msg.replyto, replyto=None,
                                                               person_id=msg.person_id, entries=[entry])
                    case messages.DBOperation.RETRIEVE_SET:
                        entries: list[dp.ScheduleEntry] = self.retrieve_schedules_for_person(msg.person_id)
                        resp_msg = messages.ScheduleEntriesMsg(destination=msg.replyto, replyto=None,
                                                               person_id=msg.person_id, entries=entries)
                    case messages.DBOperation.UPDATE:
                        self.update_schedule_last_triggered(msg.person_id, msg.seq_nbr, msg.last_triggered)
        return resp_msg

    def handle_dp_req(self, msg: Union[messages.DataPointReqMsg, messages.DataPointMsg]) -> \
            Optional[messages.DataPointRespMsg]:
        """
        Handle DataPoint requests - deletes, inserts and updates

        :param msg: a message containing the request info
        :type msg:  Union[biometrics_tracker.ipc.messages.DataPointReqMsg, biometrics_tracker.ipc.messages.DataPointMsg]
        :return: a message containing the response info
        :rtype: Optional[biometrics_tracker.ipc.messages.DataPointRespMsg]
        """
        match msg.__class__:
            case messages.DataPointReqMsg:
                match msg.operation:
                    case messages.DBOperation.RETRIEVE_SET:
                        datapoints: list[dp.DataPoint] = []
                        if msg.person_id is not None and msg.start is not None and msg.end is not None and \
                                msg.dp_type is None:
                            datapoints = self.retrieve_datapoints_for_person(msg.person_id, msg.start, msg.end)
                        else:
                            datapoints = self.retrieve_datapoints_where(msg.person_id, msg.start, msg.end, msg.dp_type)
                        return messages.DataPointRespMsg(destination=msg.replyto, replyto=None, person_id=msg.person_id,
                                                         datapoints=datapoints)
                    case messages.DBOperation.DELETE_SINGLE:
                        self.del_datapoint_per_dt_type(msg.person_id, msg.start, msg.dp_type)
                    case messages.DBOperation.DELETE_SET:
                        if msg.start is None:
                            self.del_datapoints_person(msg.person_id)
                        else:
                            self.del_datapoints_per_datetime(msg.person_id, msg.start)
            case messages.DataPointMsg:
                match msg.operation:
                    case messages.DBOperation.INSERT:
                        self.insert_datapoint(msg.payload)
                    case messages.DBOperation.UPDATE:
                        self.update_datapoint(msg.payload, msg.key_timestamp)
        return None

    def close(self):
        """
        Close the database connection

        :return: None

        """
        self.conn.close()

    def create_db(self):
        """
        Executes a series of CREATE TABLE and CREATE INDEX statements to create the database

        :return: None

        """
        curs: sql.Cursor = self.conn.cursor()
        for stmt in CREATE_STMTS:
            curs.execute(stmt)

    def drop_db(self):
        """
        Executes a series of DROP TABLE and DROP INDEX statements to un-delete the entire database

        :return: None

        """
        curs: sql.Cursor = self.conn.cursor()
        for stmt in DROP_STMTS:
            curs.execute(stmt)

    def get_connection(self, filename: str):
        """
        Establishes a connection to the sqlite3 database

        :param filename: the filename of the file containing the database
        :type filename: str
        :return: an sql.connection object
        :rtype: sqlite3.Connection

        """
        self.filename = filename
        self.conn = sql.connect(filename)
        return self.conn

    def commit(self):
        """
        Executes a COMMIT statement

        :return: None

        """
        self.conn.commit()

    def upsert_tracking_cfg(self, person_id: str, tracking_cfg: dp.TrackingConfig):
        """
        Inserts a row into the TRACK_CFG table

        :param person_id: the id of the person related to the Tracking Config info
        :type person_id: str
        :param tracking_cfg: Tracking configuration to be inserted
        :type tracking_cfg: biometrics_tracker.model.datapoints.TrackingConfig
        :return: None

        """
        curs: sql.Cursor = self.conn.cursor()
        curs.execute(INSERT_TRACK_CFG_STMT, (person_id, tracking_cfg.dp_type.name, tracking_cfg.default_uom.name))

    def delete_tracking_cfg_for_person(self, person_id):
        """
        Deletes all the rows from the TRACK_CFG table that are related to the specified person

        :param person_id: the id of the person whose entries will be deleted
        :type person_id: str
        :return: None

        """
        curs: sql.Cursor = self.conn.cursor()
        curs.execute(DELETE_TRACK_CFG_FOR_PERSON_STMT, (person_id,))

    def insert_schedule_entry(self, sched_entry: dp.ScheduleEntry):
        """
        Inserts the info in the provided biometrics_tracking.model.datapoints.ScheduleEntry
        instance to the SCHEDULE table.

        :param sched_entry: info for the schedule entry to be inserted/updated
        :type sched_entry: biometrics_tracking.model.datapoints.ScheduleEntry
        :return: None

        """
        weekdays: str = ','.join(w.name for w in sched_entry.weekdays)
        doms: str = ','.join(str(d) for d in sched_entry.days_of_month)
        when_time_str = sched_entry.when_time.strftime('%H:%M:%S')
        curs = self.conn.cursor()
        curs.execute(INSERT_SCHEDULE_STMT, (sched_entry.person_id, sched_entry.seq_nbr, sched_entry.dp_type.name,
                                            sched_entry.frequency.name, sched_entry.starts_on, sched_entry.ends_on,
                                            sched_entry.note, weekdays, doms, sched_entry.interval,
                                            when_time_str, sched_entry.suspended, sched_entry.last_triggered))
        curs.execute('COMMIT;')

    def update_schedule_entry(self, sched_entry: dp.ScheduleEntry):
        """
        Inserts the info in the provided biometrics_tracking.model.datapoints.ScheduleEntry
        instance to the SCHEDULE table.

        :param sched_entry: info for the schedule entry to be inserted/updated
        :type sched_entry: biometrics_tracking.model.datapoints.ScheduleEntry
        :return: None

        """
        weekdays: str = ','.join(w.name for w in sched_entry.weekdays)
        doms: str = ','.join(str(d) for d in sched_entry.days_of_month)
        when_time_str = sched_entry.when_time.strftime('%H:%M:%S')
        curs = self.conn.cursor()
        curs.execute(UPDATE_SCHEDULE_STMT, (sched_entry.dp_type.name, sched_entry.frequency.name, sched_entry.starts_on,
                                            sched_entry.ends_on, sched_entry.note, weekdays, doms, sched_entry.interval,
                                            when_time_str, sched_entry.suspended, sched_entry.person_id,
                                            sched_entry.seq_nbr))
        curs.execute('COMMIT;')

    def update_schedule_last_triggered(self, person_id: str, seq_nbr: int, last_triggered: datetime):
        """
        Update the last triggered date in the specified Schedule Entry

        :param person_id: ID of the person related to the Schedule Entry
        :type person_id: str
        :param seq_nbr: Sequence Number related to the Schedule Entry
        :type seq_nbr: int
        :param last_triggered: the datetime the schedule entry was triggered
        :type last_triggered: datetime
        :return: None

        """
        curs = self.conn.cursor()
        curs.execute(UPDATE_SCHEDULE_LAST_TRIGGERED_STMT, (last_triggered, person_id, seq_nbr))
        curs.execute('COMMIT;')

    def delete_schedule_for_person_seq(self, person_id: str, seq_nbr: int):
        """
        Delete a single schedule entry

        :param person_id: ID of the person whose schedule entry will be deleted
        :type person_id: str
        :param seq_nbr: sequence number of the schedule entry will be deleted
        :type seq_nbr: int
        :return: None

        """
        curs = self.conn.cursor()
        curs.execute(DELETE_SCHEDULE_FOR_PERSON_SEQ_STMT, (person_id, seq_nbr))
        curs.execute('COMMIT;')

    def delete_schedules_for_person(self, person_id: str):
        """
        Delete all schedule entries for the specified person

        :param person_id: ID of the person whose schedule entries will be deleted
        :type person_id: str
        :return: None

        """
        curs = self.conn.cursor()
        curs.execute(DELETE_SCHEDULES_FOR_PERSON_STMT, (person_id,))
        curs.execute('COMMIT;')

    @staticmethod
    def build_schedule(person_id: str, row: sql.Row) -> dp.ScheduleEntry:
        """
        Creates a ScheduleEntry instance from a row retrieved from the SCHEDULE table

        :param person_id: the ID of the Person whose Schedule entries were retrieved
        :type person_id: str
        :param row: the row retrieved from the database
        :type row: sql.Row
        :return: the ScheduleEntry instance
        :rtype: biometrics_tracker.model.datapoints.ScheduleEntry

        """
        seq_nbr, dp_type_str, freq_str, starts_on_str, ends_on_str, note, weekdays_str, doms_str, interval, \
            when_time_str, suspended_int, last_triggered_str = row
        dp_type: dp.DataPointType = dp.dptype_name_map[dp_type_str]
        frequency: dp.FrequencyType = dp.frequency_name_map[freq_str]
        starts_on: date = utilities.datetime_from_str(starts_on_str).date()
        ends_on: date = utilities.datetime_from_str(ends_on_str).date()
        when_time: Optional[time] = utilities.time_from_str(when_time_str)
        suspended: bool = False
        if suspended_int == 1:
            suspended = True
        last_triggered: Optional[datetime] = utilities.datetime_from_str(last_triggered_str)
        weekdays: list[dp.WeekDay] = []
        if len(weekdays_str.strip()) > 0:
            for wd in weekdays_str.split(','):
                weekdays.append(dp.weekday_name_map[wd])
        doms: list[int] = []
        if len(doms_str.strip()) > 0:
            for dom_str in doms_str.split(','):
                doms.append(int(dom_str))
        return dp.ScheduleEntry(person_id=person_id, seq_nbr=seq_nbr, frequency=frequency, dp_type=dp_type,
                                starts_on=starts_on, ends_on=ends_on, note=note, weekdays=weekdays,
                                days_of_month=doms, interval=interval, when_time=when_time,
                                suspended=suspended, last_triggered=last_triggered)

    def retrieve_schedules_for_person(self, person_id) -> list[dp.ScheduleEntry]:
        """
        Retrieve the schedule entries associated with the specified person

        :param person_id: ID of the person whose schedule entries will be retrieved
        :type person_id: str
        :return: biometrics_tracker.ipc.messages.ScheduleEntriesMsg

        """
        curs = self.conn.cursor()
        rows = curs.execute(RETRIEVE_SCHEDULES_FOR_PERSON_STMT, (person_id,))
        entries: list[dp.ScheduleEntry] = []
        for row in rows:
            sched_entry: dp.ScheduleEntry = self.build_schedule(row)
            entries.append(sched_entry)
        return entries

    def retrieve_schedule_for_person_seq(self, person_id: str, seq_nbr: int) -> Optional[dp.ScheduleEntry]:
        """
        Retrieves a single ScheduleEntry matching the supplied Person ID and SecheduleEntry sequence number

        :param person_id: ID of the person whose ScheduleEntry will be retrieved
        :type person_id: str
        :param seq_nbr: sequence number of the ScheduleEntry to be retrieved
        :type seq_nbr: int
        :return: the retrieved ScheduleEntry or None
        :rtype: Optional[biometrics_tracker.model.datapoints.ScheduleEntry]

        """
        curs = self.conn.cursor()
        row: sql.Row = curs.execute(RETRIEVE_SCHEDULE_FOR_PERSON_SEQ_STMT, (person_id, seq_nbr)).fetchone()
        if row is not None:
            sched_entry = self.build_schedule(person_id=person_id, row=row)
            return sched_entry
        else:
            return None

    def insert_person(self, person: dp.Person):
        """
        Inserts the info in the provided model.datapoints.Person instance to the PEOPLE table

        :param person: info for person to be inserted
        :type person: biometrics_tracker.model.datapoints.Person
        :return: None

        """
        curs: sql.Cursor = self.conn.cursor()
        curs.execute(INSERT_PERSON_STMT, (person.id, person.name, person.dob))
        for tracking_cfg in person.tracked.values():
            self.upsert_tracking_cfg(person.id, tracking_cfg)
        curs.execute('COMMIT;')

    def update_person(self, person: dp.Person):
        """
        Updates a PERSON table row with the provided model.datapoints.Person instance

        :param person: info for person to be updated
        :type person: biometrics_tracker.model.datapoints.Person
        :return: None

        """
        self.delete_tracking_cfg_for_person(person.id)
        curs: sql.Cursor = self.conn.cursor()
        curs.execute(UPDATE_PERSON_STMT, (person.name, person.dob, person.id))
        for tracking_cfg in person.tracked.values():
            self.upsert_tracking_cfg(person.id, tracking_cfg)
        curs.execute('COMMIT;')

    def retrieve_person(self, person_id: str):
        """
        Retrieve a row from the PEOPLE table for the specified person id

        :param person_id: The id of the row to be retrieved
        :type person_id: str
        :return: retrieved Person info
        :rtype: biometrics_tracker.model.datapoints.Person

        """
        curs: sql.Cursor = self.conn.cursor()
        row: sql.Row = curs.execute(RETRIEVE_PERSON_STMT, (person_id,)).fetchone()
        name, dob_str = row
        dob_date = date(int(dob_str[0:4]), int(dob_str[5:7]), int(dob_str[8:11]))
        age_dur = date.today() - dob_date
        age = age_dur.days // 365
        person: dp.Person = dp.Person(person_id, name, dob_date, age)
        for row in curs.execute(RETRIEVE_TRACK_CFG_FOR_PERSON_STMT, (person_id,)).fetchall():
            dptype_name, uom_name = row
            track_cfg: dp.TrackingConfig = dp.TrackingConfig(dp.dptype_name_map[dptype_name],
                                                             uoms.uom_map[uom_name], True)
            person.add_tracked_dp_type(track_cfg.dp_type, track_cfg)

        return person

    def insert_datapoint(self, datapoint: dp.DataPoint):
        """
        Inserts the data from an instance of a subclass of model.datapoints.DataPoint to the DATAPOINTS table

        :param datapoint: datapoint info to be inserted
        :type datapoint: biometrics_tracker.model.datapoints.DataPoint
        :return: None

        """
        data_json = json.dumps(datapoint.data, cls=jh.BiometricsJSONEncoder)
        curs: sql.Cursor = self.conn.cursor()
        curs.execute(INSERT_DATAPOINT_STMT, (datapoint.person_id, datapoint.taken, datapoint.note, data_json,
                                             datapoint.type.value))
        curs.execute('COMMIT;')

    def update_datapoint(self, datapoint: dp.DataPoint, key_timestamp: datetime):
        """
        Updates a DATAPOINTS table row with the data from an instance of a subclass of model.datapoints.DataPoint

        :param datapoint: an datapoint to be upated
        :type datapoint: an instance of one of the classes in model.datapoints.dptypes_union
        :return: None

        """
        data_json = json.dumps(datapoint.data, cls=jh.BiometricsJSONEncoder)
        curs: sql.Cursor = self.conn.cursor()
        curs.execute(UPDATE_DATAPOINT_STMT, (datapoint.taken, datapoint.note, data_json, datapoint.person_id,
                                             key_timestamp, datapoint.type.value))
        curs.execute('COMMIT;')

    def del_datapoints_person(self, person_id: str):
        curs: sql.Cursor = self.conn.cursor()
        curs.execute(DELETE_DP_FOR_PERSON_STMT, (person_id, ))
        curs.execute('COMMIT')

    def del_datapoints_per_datetime(self, person_id: str, taken: datetime):
        curs: sql.Cursor = self.conn.cursor()
        curs.execute(DELETE_DP_FOR_PER_DATETIME, (person_id, taken))
        curs.execute('COMMIT')

    def del_datapoint_per_dt_type(self, person_id: str, taken: datetime, type: dp.DataPointType):
        curs: sql.Cursor = self.conn.cursor()
        curs.execute(DELETE_DP_FOR_PER_DT_TYPE, (person_id, taken, type.value))
        curs.execute('COMMIT')

    def retrieve_datapoints_for_person(self, person_id: str, start_date: datetime,
                                       end_date: datetime) -> list[dp.DataPoint]:
        """
        Retrieves rows related to the specified person id from the DATAPOINTS table, returning a list of
        instances of subclasses of model.datapoints.DataPoint

        :param person_id: indicates the person whose data is to be retrieved
        :type person_id: str
        :param start_date: datetime of the first row to be retrieved
        :type start_date: datetime.datetime
        :param end_date: datetime of the last row to be retrieved
        :type end_date: datetime.datetime
        :return: a list of instances of subclasses of model.datapoints.DataPoint
        :rtype: list[model.datapoints.UOM]

        """
        curs: sql.Cursor = self.conn.cursor()
        rows = curs.execute(RETRIEVE_DP_FOR_PERSON_STMT, (person_id, start_date, end_date)).fetchall()
        return DataBase.retrieve_datapoints(rows)

    def retrieve_datapoints_where(self,  person_id: Optional[str] = None,
                                  start_datetime: Optional[datetime] = None,
                                  end_datetime: Optional[datetime] = None,
                                  dp_type: dp.DataPointType = None) -> list[dp.DataPoint]:
        """
        This method was created to provide more flexibility in the selection criteria for DataPoints.  Selection
        for Person, date range and DataPointType can be mixed and matches.  A parameter value of None indicates
        that the column will not be included in the SELECT statement where clause.

        :param person_id: the optional ID of the person to select rows for
        :type person_id: Optional[str]
        :param start_datetime: the optional earliest date/time to select rows for
        :type start_datetime: Optional[datetime]
        :param end_datetime: the optional most recent date/time to select rows for
        :type end_datetime: Optional[datetime]
        :param dp_type: the optional DataPointType to select rows for
        :type dp_type: Optional[biometrics_tracker.model.datapoints.DataPointType]
        :return: a list of DataPoints that meet the provided selection criterial
        :rtype: list[biometrics_tracker.model.datapoints.DataPoint]

        """

        where_clause: str = ''
        values: tuple = ()
        if not (person_id is None and start_datetime is None and end_datetime is None and dp_type is None):
            if person_id is not None:
                where_clause = f'{where_clause} PERSON_ID = ?'
                values = (person_id,)
            if start_datetime is not None:
                if len(values) > 0:
                    where_clause = f'{where_clause} AND TIMESTAMP >= ?'
                else:
                    where_clause = f'{where_clause} TIMESTAMP >= ?'
                values = (*values, start_datetime)
            if end_datetime is not None:
                if len(values) > 0:
                    where_clause = f'{where_clause} AND TIMESTAMP <= ?'
                else:
                    where_clause = f'{where_clause} TIMESTAMP <= ?'
                values = (*values, end_datetime)
            if dp_type is not None:
                if len(values) > 0:
                    where_clause = f'{where_clause} AND TYPE = ?'
                else:
                    where_clause = f'{where_clause} TYPE = ?'
                values = (*values, dp_type.value)
            stmt = f'{RETRIEVE_DP_STMT} WHERE {where_clause}'
        else:
            stmt = RETRIEVE_DP_STMT
        curs: sql.Cursor = self.conn.cursor()
        rows = curs.execute(stmt, values).fetchall()
        return DataBase.retrieve_datapoints(rows)

    @staticmethod
    def retrieve_datapoints(rows: list) -> list[dp.DataPoint]:
        """
        Creates a list of DataPoints from rows retrieved from the DATAPOINTS table

        :param rows: the rows retrieved by a SELECT statement
        :type rows: list[list[tuple[]]]
        :return: a list of DataPoints
        :rtype:  list[biometrics_tracker.model.datapoints.DataPoint]

        """
        datapoints: list[dp.DataPoint] = []
        for row in rows:
            person_id, ts_str, dp_type_int, note, data_json = row
            ts = datetime(int(ts_str[0:4]), int(ts_str[5:7]), int(ts_str[8:10]), int(ts_str[11:13]),
                          int(ts_str[14:16]), int(ts_str[17:19]))
            data = json.loads(eval(data_json), object_hook=jh.biometrics_object_hook)
            dp_type = dp.DataPointType(dp_type_int)
            datapoints.append(dp.DataPoint(person_id, ts, note, data, dp_type))
        return datapoints

    def retrieve_people(self) -> list[tuple[str, str]]:
        """
        Retrieves a list of (id, name) tuples from the PEOPLE table.  This list can be used to implement
        a selection list for the people whose biometrics are tracked by the application

        :return: a list of (string, string) tuples, the first element being the person id, the second the person name
        :rtype: list[tuple[str, str]]

        """
        people_list: list[tuple[str, str]] = []
        curs: sql.Cursor = self.conn.cursor()
        for row in curs.execute(RETRIEVE_PEOPLE_STMT):
            id, name = row
            people_list.append((id, name))
        return people_list



