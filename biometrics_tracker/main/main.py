"""
This script is the entry point for the Biometrics Tracking application.  It attempts to open a JSON formatted
file which contains the application's configuration info.  If this file is not found, which will happen the first
time the application is invoked, the config.createconfig.ConfigGUI Window will be presented to allow configuration
parameters to be set.  If the import succeeds, the gui.Application Window will be presented.
"""
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
        scheduler = sched.Scheduler(config_info, self.queue_mgr)
        scheduler.start()
        return True

    def launch_scheduled_entry(self, config_path: pathlib.Path, schedule_str: str, person_id: str,
                               dp_type_name:str, schedule_note: str) -> bool:
        """
        Launch the Biometric Tracker GUI

        :param config_path: a pathlib object connected to the configuration file
        :type config_path: pathlib.Path
        :param schedule_str: a string representation of the Schedule that triggered the Scheduled Entry session
        :type schedule_str: str
        :param person_id: the id of the person whose metric will be recorded
        :type person_id: str
        :param dp_type_name: the string representation of the DataPointType for the metric
        :type dp_type_name: str
        :param schedule_note: the note associated with the Schedule that triggered the Schedule Entry session
        :type schedule_note: str
        :return: quit flag
        :rtype: bool

        """
        config_info = self.pre_launch(config_path)
        print(f'schedule_str {schedule_str}')
        print(f'person_id {person_id}')
        print(f'dp_type_name {dp_type_name}')
        print(f'schedule_note {schedule_note}')
        app = gui.ScheduledEntryWindow(config_info=config_info, queue_mgr=self.queue_mgr,
                                       schedule_str=schedule_str, person_id=person_id, dp_type_name=dp_type_name,
                                       schedule_note=schedule_note)
        app.mainloop()
        app.destroy()
        self.queue_mgr.send_db_req_msg(messages.CloseDataBaseReqMsg(destination=per.DataBase, replyto=None))
        return True


def launch():
    """
    Check for a configuration file.  If one is found, launch the Biometrics Tracking GUI or Scheduler of Config GUI,
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
            if len(sys.argv) > 1:
                match sys.argv[1]:
                    case '--config':
                        quit = launcher.launch_config(homepath)
                    case '--gui':
                        quit = launcher.launch_gui(config_path)
                    case '--scheduler':
                        quit = launcher.launch_scheduler(config_path)
                    case '--scheduled-entry':
                        quit = launcher.launch_scheduled_entry(config_path=config_path,
                                                               schedule_str=sys.argv[2],
                                                               person_id=sys.argv[3],
                                                               dp_type_name=sys.argv[4],
                                                               schedule_note=sys.argv[5])
                    case _:
                       quit = launcher.launch_gui(config_path)
            else:
                quit = launcher.launch_gui(config_path)
        else:
            quit = launcher.launch_config(homepath)

    sys.exit()


if __name__ == '__main__':
    launch()
