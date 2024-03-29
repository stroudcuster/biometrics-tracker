"""
This script is the entry point for the Biometrics Tracking application.  It attempts to open a JSON formatted
file which contains the application's configuration info.  If this file is not found, which will happen the first
time the application is invoked, the config.createconfig.ConfigGUI Window will be presented to allow configuration
parameters to be set.  If the import succeeds, the gui.Application Window will be presented.
"""
import argparse
import json
import logging
import logging.config
import os
import pathlib
import sys

import ttkbootstrap as ttkb
from ttkbootstrap.constants import *
from typing import Optional

import biometrics_tracker.config.createconfig as config
import biometrics_tracker.ipc.messages as messages
import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.config.json_handler as jh
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.persistence as per
import biometrics_tracker.main.scheduler as sched
import biometrics_tracker.gui.tk_gui as gui


class ErrorDialog(ttkb.Window):
    """
    Used to present error messages to the user
    """
    def __init__(self, message: str):
        """
        Creates an instance of ErrorDialog

        :param message: the message to be displayed
        :type message: str

        """
        ttkb.Window.__init__(self, title='Biometrics Tracker Setup', themename='darkly')
        ttkb.Label(self, text=message).grid(row=0, column=0, padx=5, pady=5, sticky=NW)
        ttkb.Button(self, text="OK", command=self.close, default='normal').grid(row=1, column=0, padx=5, pady=5,
                                                                                sticky=NE)
        self.grid()
        self.mainloop()

    def close(self):
        """
        Close the error message GUI

        :return: None

        """
        self.quit()
        self.withdraw()


class Launcher:
    """
    Launches the Biometrics Tracker app, or if this is the first time invocation, prompt for configuation info,
    then start the application
    """
    def __init__(self, queue_mgr: queues.Queues):
        """
        Create and instance of Launcher

        :param queue_mgr: the queue manager object
        :type queue_mgr: biometrics_tracker.ipc.queue_mgr.Queues

        """
        self.queue_mgr = queue_mgr
        self.db: Optional[per.DataBase] = None
        self.person: Optional[dp.Person] = None
        self.args: Optional[argparse.Namespace] = None

    def start_database(self, filename: str):
        """
        Start the database thread

        :param filename: The file name of the SQLite3 database
        :type filename: str
        :return: None

        """
        self.db = per.DataBase(filename, self.queue_mgr, block_req_queue=True)
        self.db.start()

    def pre_launch(self, config_path: pathlib.Path) -> config.ConfigInfo:
        with config_path.open(mode='tr', encoding='UTF-8') as cfg:
            config_json = cfg.read()
            config_info: config.ConfigInfo = json.loads(eval(config_json), object_hook=jh.config_object_hook)
        logging.config.dictConfig(config_info.logging_config)
        if self.db is None:
            self.start_database(pathlib.Path(config_info.db_dir_path, 'biometrics.db').__str__())
        return config_info

    def launch_gui(self, config_path: pathlib.Path) -> bool:
        """
        Launch the Biometric Tracker GUI

        :param config_path: a pathlib object connected to the configuration file
        :type config_path: pathlib.Path
        :return: quit flag
        :rtype: bool

        """
        config_info = self.pre_launch(config_path)
        app = gui.Application(config_info, self.queue_mgr)
        app.mainloop()
        app.destroy()
        if app.dispatcher is not None:
            app.dispatcher.join()
        self.queue_mgr.send_db_req_msg(messages.CloseDataBaseReqMsg(destination=per.DataBase, replyto=None))
        return True

    def launch_config(self, homepath: pathlib.Path) -> bool:
        """
        Launch a GUI to prompt the user for configuration info

        :param homepath: a pathlib object associated with the user's home directory
        :type homepath: pathlib.Path
        :return: quit flag
        :rtype: bool

        """
        app_dir_path: pathlib.Path = pathlib.Path(homepath, 'biometrics-tracker')
        config_gui = config.ConfigGUI(app_dir_path)
        config_info = config_gui.ask_config()
        quit: bool = True
        if config_info.db_dir_path is not None:
            self.start_database(config_info.db_dir_path.name)
            self.queue_mgr.send_db_req_msg(messages.CreateDataBaseReqMsg(destination=per.DataBase,
                                                                         replyto=None))
            msg: messages.CompletionMsg = self.queue_mgr.check_completion_queue(block=True)
            self.queue_mgr.send_db_req_msg(messages.CloseDataBaseReqMsg(destination=per.DataBase,
                                                                        replyto=None))
            if not msg.status == messages.Completion.SUCCESS:
                ErrorDialog(message='Database creation failed. The Biometrics Tracker can not be started.')
                quit = True
        else:
            quit = True
        return quit

    def launch_scheduler(self, config_path: pathlib.Path) -> bool:
        """
        Launch the Biomentrics Tracker Scheduler

        :param config_path: a Path object pointing to the configuration file
        :type config_path: pathlib.Path
        :return: quit flag
        :rtype: bool

        """
        config_info: config.ConfigInfo = self.pre_launch(config_path)
        scheduler = sched.Scheduler(config_info, self.queue_mgr, start_dispatcher=True)
        scheduler.start()
        return True

    def launch_scheduled_entry(self, config_path: pathlib.Path) -> bool:
        """
        Launch the Biometric Tracker GUI

        :param config_path: a pathlib object connected to the configuration file
        :type config_path: pathlib.Path
        :return: quit flag
        :rtype: bool

        """
        def retrieve_schedule(msg: messages.ScheduleEntriesMsg):
            schedule = msg.entries[0]
            app = gui.ScheduledEntryWindow(config_info=config_info, queue_mgr=self.queue_mgr,
                                           schedule=schedule, person=self.person)
            app.mainloop()
            app.destroy()
            self.queue_mgr.send_db_req_msg(messages.CloseDataBaseReqMsg(destination=per.DataBase, replyto=None))

        def retrieve_person(msg: messages.PersonMsg):
            self.person = msg.payload
            self.queue_mgr.send_db_req_msg(messages.ScheduleEntryReqMsg(destination=per.DataBase,
                                                                        replyto=retrieve_schedule,
                                                                        person_id=self.person.id,
                                                                        seq_nbr=self.args.seq[0],
                                                                        last_triggered=None,
                                                                        operation=messages.DBOperation.RETRIEVE_SINGLE))

        config_info = self.pre_launch(config_path)
        self.queue_mgr.send_db_req_msg(messages.PersonReqMsg(destination=per.DataBase,
                                                             replyto=retrieve_person,
                                                             person_id=self.args.id[0],
                                                             operation=messages.DBOperation.RETRIEVE_SINGLE))
        done: bool = False
        while not done:
            msg = self.queue_mgr.check_db_resp_queue(block=True)
            if msg is not None:
                msg.destination(msg)
            if msg.__class__ == messages.ScheduleEntriesMsg:
                done = True
        return True


def launch():
    """
    Check for a configuration file.  If one is found, launch the Biometrics Tracking GUI or Scheduler or Config GUI,
    according to the command line argument.  If not, launch the Config GUI

    """

    quit: bool = False
    if sys.platform[0:3] == 'win':
        home_str = os.environ['HOMEPATH']
    else:
        home_str = os.environ['HOME']
    homepath: pathlib.Path = pathlib.Path(home_str)
    queue_mgr = queues.Queues(sleep_seconds=.5)
    launcher = Launcher(queue_mgr)
    while not quit:
        config_path: pathlib.Path = pathlib.Path(homepath, os.sep.join(['biometrics-tracker', 'config',
                                                                        'config_info.json']))
        if config_path.exists():
            arg_parser = argparse.ArgumentParser(prog='biotrack')
            sub_parser = arg_parser.add_subparsers(title='commands', dest='subcmd',
                                                   metavar='config | gui | scheduler | scheduled-entry',
                                                   help='config = intitial configuration,  gui = start application GUI,'
                                                        '  scheduler = start scheduler process,  scheduled-entry = '
                                                        'initiate Scheduled Entry session',
                                                   required=True)

            config_parser = sub_parser.add_parser('config')
            config_parser.set_defaults(func=lambda hp=homepath: launcher.launch_config(homepath=hp))
            scheduler_parser = sub_parser.add_parser('scheduler')
            scheduler_parser.set_defaults(func=lambda cp=config_path: launcher.launch_scheduler(config_path=cp))
            sched_entry_parser = sub_parser.add_parser('scheduled-entry')
            sched_entry_parser.set_defaults(func=lambda cp=config_path: launcher.launch_scheduled_entry(config_path=cp))
            sched_entry_parser.add_argument(dest='id', type=str, nargs=1,
                                            help='Person ID')
            sched_entry_parser.add_argument(dest='seq', type=int, nargs=1,
                                            help='Schedule sequence number')
            gui_parser = sub_parser.add_parser('gui')
            gui_parser.set_defaults(func=lambda cp=config_path: launcher.launch_gui(config_path=cp))

            launcher.args = arg_parser.parse_args()
            quit = launcher.args.func()

        else:
            quit = launcher.launch_config(homepath)

    sys.exit()


if __name__ == '__main__':
    launch()
