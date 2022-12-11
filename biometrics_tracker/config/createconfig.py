import json
import pathlib
from tkinter import ttk
import tkinter.font as font
import ttkbootstrap as ttkb
from typing import Optional

import biometrics_tracker.config.json_handler as jh
import biometrics_tracker.config.logging_config as logging_config
import biometrics_tracker.utilities.utilities as util


class ConfigInfo:
    """
    A container for the application's configuration info

    """
    def __init__(self, config_file_path: Optional[pathlib.Path] = None, db_dir_path: Optional[pathlib.Path] = None,
                 menu_font_size: int = 12, default_font_size: int = 12, text_font_size: int = 12,
                 log_config: Optional[logging_config.LoggingConfig] = None,
                 help_url: Optional[str] = None, plugin_dir_path: Optional[pathlib.Path] = None):
        self.config_file_path: Optional[pathlib.Path] = config_file_path
        self.db_dir_path: Optional[pathlib.Path] = db_dir_path
        self.menu_font_size = menu_font_size
        self.default_font_size = default_font_size
        self.text_font_size = text_font_size
        self.help_url: Optional[str] = help_url
        self.plugin_dir_path: Optional[pathlib.Path] = plugin_dir_path
        if log_config is None:
            self.logging_config = {}
        else:
            self.logging_config: dict = log_config.config

    def is_complete(self) -> bool:
        """
        Has the user supplied the required configuration info

        :return: True if the info is present, False otherwise
        :rtype: bool

        """
        complete: bool = True
        if self.menu_font_size == 0 or self.default_font_size == 0 or self.text_font_size == 0:
            complete = False
        return complete

    def pickle_path(self) -> pathlib.Path:
        return self.config_file_path.parent

    def write_json(self):
        with self.config_file_path.open(mode='tw', encoding='UTF-8') as cfg_path:
            data_json = json.dumps(self, cls=jh.ConfigJSONEncoder)
            print(data_json, file=cfg_path)
            cfg_path.flush()


class ConfigGUI(ttkb.Window):
    """
    A GUI that allows entry/maintenance of the application's configuration info
    """
    def __init__(self, app_dir_path: pathlib.Path):
        ttkb.Window.__init__(self, title="Biometrics Tracker Configuration", themename="darkly",
                             iconphoto=pathlib.Path(util.whereami('biometrics_tracker'), 'gui',
                                                    'biometrics_tracker.png').__str__())
        width = 500
        height = 200
        screenwidth = self.winfo_screenwidth()
        screenheight = self.winfo_screenheight()
        alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
        self.geometry(alignstr)
        font.nametofont('TkMenuFont').config(size=14)
        font.nametofont('TkTextFont').config(size=14)
        font.nametofont('TkDefaultFont').config(size=14)
        self.app_dir_path = app_dir_path
        self.config_dir_path = None
        ttkb.Label(self, text='Font Size').grid(column=0, row=3, padx=5, pady=5)
        self.font_size_var = ttkb.IntVar()
        self.font_size_entry = ttk.Combobox(self, textvariable=self.font_size_var, values=['10', '12', '14', '16'])
        self.font_size_entry.grid(column=1, row=3, padx=5, pady=5)
        self.font_size_entry.bind('<<ComboboxSelected>>', self.set_font_size)
        self.font_size_entry.current(newindex=2)
        self.font_size = 10
        ttkb.Button(self, text="Create Config", command=self.create_config).grid(column=5, row=6, padx=5, pady=5)
        self.config_info = ConfigInfo()

    def ask_config(self) -> ConfigInfo:
        """
        Displays the GUI and starts the event loop

        :return: an instance of ConfigInfo
        :rtype: biometrics_tracker.config.createconfig.ConfigInfo

        """
        self.mainloop()
        return self.config_info

    def set_font_size(self, event):
        """
        Retrieves the font size value entered by the user.  This method is bound to the combo box selection
        event.

        :param event:
        :type event: ttkbootstrap.Event
        :return: None

        """
        if event:
            pass
        self.font_size = self.font_size_var.get()

    def create_config(self):
        """
        Using the info entered to the GUI to create a Python script that creates an instance of config.ConfigInfo

        :return: None

        """
        if not self.app_dir_path.exists():
            self.app_dir_path.mkdir(parents=True)
        self.config_dir_path = pathlib.Path(self.app_dir_path, 'config')
        if not self.config_dir_path.exists():
            self.config_dir_path.mkdir(parents=True)
        self.config_info.db_dir_path = pathlib.Path(self.app_dir_path, 'data')
        if not self.config_info.db_dir_path.exists():
            self.config_info.db_dir_path.mkdir(parents=True)
        log_dir_path: pathlib.Path = pathlib.Path(self.app_dir_path, 'logs')
        if not log_dir_path.exists():
            log_dir_path.mkdir(parents=True)
        if 'version' in self.config_info.logging_config:
            log_config = logging_config.LoggingConfig(self.config_info.logging_config)
        else:
            log_config = logging_config.LoggingConfig(config=None)
        log_config.set_log_filename(pathlib.Path(log_dir_path, 'biometrics.log').__str__())
        self.config_info.logging_config = log_config.config
        self.config_info.menu_font_size = self.font_size_var.get()
        self.config_info.default_font_size = self.font_size_var.get()
        self.config_info.text_font_size = self.font_size_var.get()
        self.config_info.help_url = pathlib.Path(util.whereami('biometrics_tracker'), 'help',
                                                 'user-doc-index.html').as_uri()
        self.config_info.plugin_dir_path = pathlib.Path(self.config_dir_path, 'plugins')
        self.config_info.config_file_path = pathlib.Path(self.config_dir_path, 'config_info.json')
        self.config_info.write_json()
        self.quit()
        self.destroy()
