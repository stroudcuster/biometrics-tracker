from abc import abstractmethod
from datetime import datetime, date, time
from io import StringIO, SEEK_SET
import pathlib
import re
import tkinter as tk
import tkinter.filedialog as filedialog
import tkinter.font as font
from tkinter import ttk
from typing import Any, Callable, Optional, Union
import webbrowser

import ttkbootstrap as ttkb
import ttkbootstrap.dialogs.dialogs as dialogs
import ttkbootstrap.scrolled as scrolled
from ttkbootstrap.constants import *

import biometrics_tracker.config.createconfig as config
import biometrics_tracker.main.core as core
import biometrics_tracker.main.scheduler as scheduler
import biometrics_tracker.main.dispatcher as dispatcher
import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.ipc.messages as messages
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.exporters as exp
import biometrics_tracker.model.importers as imp
import biometrics_tracker.model.persistence as per
import biometrics_tracker.output.reports as reports
from biometrics_tracker.utilities.utilities import mk_datetime, increment_date, split_camelcase, \
    whereami, compare_mdyhms
import biometrics_tracker.version as version
import biometrics_tracker.gui.widgets as widgets
from biometrics_tracker.gui.widgets import DateWidget, TimeWidget, DataPointTypeWidget, ImportExportFieldWidget, \
    dp_widget_union, MetricWidgetFactory, Checkbutton, Radiobutton


class PersonFrame(ttkb.Frame):
    """
    A ttkbootstrap Frame that allows the entry of information associated with a person, including which metrics
    will be tracked for that person

    """
    def __init__(self, parent, title: str, cancel_action: Callable, submit_action: Callable, person: dp.Person = None):
        """
        Creates a gui.PersonFrame instance

        :param parent: the GUI parent for this Fram
        :type parent: Tk derived Window or Frame
        :param title: the title associated with the frame
        :type title: str
        :param cancel_action: a Callable to be invoked when the cancel button is pressed
        :type cancel_action: Callable
        :param submit_action: a Callable to be invoked when the submit button is pressed
        :type cancel_action: Callable
        :param person: info for the person to be edited, defaults to None
        :type person: biometrics_tracker.model.datapoints.Person

        """
        ttkb.Frame.__init__(self, parent)
        self.rowconfigure("all", weight=1)
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=3)
        self.person_id_var = tk.StringVar()
        self.name_var = tk.StringVar()
        ttkb.Label(self, text=title).grid(column=0, row=0, columnspan=3)
        startdate: date = date.today()
        if person:
            self.person_id_var.set(person.id)
            self.name_var.set(person.name)
            startdate = person.dob
        ttk.Label(self, text="Person ID:", anchor=tk.E).grid(column=0, row=1, sticky=NSEW, padx=5, pady=5)
        self.id_entry = ttkb.Entry(self, textvariable=self.person_id_var, width=20)
        self.id_entry.grid(column=1, row=1, sticky=tk.W, rowspan=2, pady=5, padx=5)
        ttk.Label(self, text="Name:", anchor=tk.E).grid(column=0, row=3, sticky=NSEW, rowspan=1)
        self.name_entry = ttkb.Entry(self, textvariable=self.name_var, width=50)
        self.name_entry.grid(column=1, row=3, sticky=tk.W, rowspan=1, pady=5, padx=5)
        ttk.Label(self, text="Date of Birth:", anchor=tk.E).grid(column=0, row=4, sticky=NSEW, rowspan=1)
        self.date_entry: DateWidget = DateWidget(self, startdate)
        self.date_entry.grid(column=1, row=4, rowspan=1, pady=5, padx=5, sticky=tk.W)
        self.dp_widgets: list[DataPointTypeWidget] = []
        self.date_entry.set_prev_entry(self.name_entry)

        ttk.Label(self, text="Check the items you will be tracking and select a unit of measure for each",
                  wraplength=500, anchor=tk.W).grid(column=0, row=5, columnspan=3, rowspan=2, pady=10,
                                                    padx=5, sticky=NW)
        row = 7
        for dp_type in dp.dptype_dp_map.keys():
            tracked = False
            if person and person.is_tracked(dp_type):
                tracked = True
            dp_widget = DataPointTypeWidget(self, dp_type, dp.dptype_uom_map[dp_type], tracked)
            if self.date_entry.next_entry is None:
                self.date_entry.next_entry = dp_widget
            if tracked:
                track_cfg = person.tracked[dp_type]
                dp_widget.uom_widget.set_selection(track_cfg.default_uom)
            dp_widget.grid(column=0, row=row, columnspan=2, pady=5, sticky=NW)
            self.dp_widgets.append(dp_widget)
            row += 1

        self.date_entry.set_next_entry(self.dp_widgets[0])
        ttkb.Button(self, text="Cancel", default="normal", command=cancel_action) \
            .grid(column=0, row=row, pady=5, padx=5, sticky=NE)
        ttkb.Button(self, text="Save", default="normal", command=submit_action) \
            .grid(column=1, row=row, pady=5, padx=5, sticky=NE)
        self.grid(padx=150)


class PeopleListFrame(ttkb.Frame):
    """
    A ttkbootstrap Frame containing a tk.Listbox that allows the selection of a person whose data is to be used in
    subsequent processes
    """
    def __init__(self, parent, title, people_list: list[tuple[str, str]], queue_mgr: queues.Queues, callback: Callable):
        """
        Creates an instance of gui.PeopleListFrame

        :param parent: the GUI parent for this Frame
        :type parent: Tk derived Window or Frame
        :param title: a title to be display at the top of the frame
        :type title: str
        :param people_list: id's and names for the people to be displayed in the list
        :type people_list: list[tuple[str, str]]
        :param callback: callback to be invoked when a Listbox entry is selected
        :type callback: Callable

        """
        ttkb.Frame.__init__(self, parent)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        ttkb.Label(self, text=title).grid(column=0, row=0, padx=5, pady=5, sticky=tk.EW)
        self.people_list: list[tuple[str, str]] = people_list
        self.queue_mgr: queues.Queues = queue_mgr
        self.callback: Callable = callback
        self.listbox: Optional[tk.Listbox] = None
        # Don't present the list if there's only one person in it.  Select that person automatically.  See the
        # selected_person method
        if len(self.people_list) != 1:
            self.listbox = tk.Listbox(self)
            for idx, id_name in enumerate(people_list):
                self.listbox.insert(idx, f"{id_name[0]} {id_name[1]}")
            self.listbox.bind('<Double-1>', self.send_db_req)
            self.listbox.grid(column=0, row=1, padx=5, pady=5, stick=tk.NSEW)
            self.grid()
        else:
            self.send_db_req(event=None)

    def focus_set(self):
        """
        Gives the Frame's Listbox focus

        :return: None
        :rtype: None

        """
        if self.listbox is not None:
            self.listbox.focus_set()

    def selected_person(self) -> str:
        """
        Returns the Person ID for the selected Listbox entry

        :return: person ID
        :rtype: str

        """
        if len(self.people_list) != 1:
            idx, = self.listbox.curselection()
            return self.people_list[idx][0]
        else:
            # If there's only one entry in the list, automatically select it
            return self.people_list[0][0]

    def send_db_req(self, event: Optional[tk.Event]=None):
        self.queue_mgr.send_db_req_msg(messages.PersonReqMsg(destination=per.DataBase, replyto=self.callback,
                                                             person_id=self.selected_person(),
                                                             operation=messages.DBOperation.RETRIEVE_SINGLE))


class NewDataPointFrame(ttkb.Frame):
    """
    A ttkbootstrap Frame that allows entry of a set of metrics for a particular date and time
    """
    def __init__(self, parent, person: dp.Person, cancel_action: Callable, submit_action: Callable):
        """
        Creates an instance of gui.QuickEntryFrame

        :param parent: the GUI parent for this Frame
        :type parent: Tk derived Window or Frame
        :param person:  data associated with the person whose metrics are to be recorded
        :type person: biometrics_tracker.model.datapoints.Person
        :param cancel_action: a callback to be invoked when the cancel button is pressed
        :type cancel_action: Callable
        :param submit_action: a callback to be invoked when the submit button is pressed
        :type submit_action: Callable
        """
        ttkb.Frame.__init__(self, parent)
        self.person: dp.Person = person
        self.bio_entries = []
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=3)
        self.columnconfigure(2, weight=1)
        self.columnconfigure(3, weight=1)
        self.columnconfigure(4, weight=3)
        row: int = 0
        ttkb.Label(self, text="Add New Datapoint").grid(column=0, row=row, columnspan=4)
        row += 1
        ttkb.Label(self, text=f'Person: {self.person.id} {self.person.name} DOB: {self.person.dob.month}/'
                   f'{self.person.dob.day}/{self.person.dob.year}').grid(column=0, row=row,
                                                                         columnspan=4, padx=5, pady=5)
        row += 1
        ttkb.Label(self, text="Date:", anchor=tk.W).grid(column=0, row=row, padx=5, pady=5)
        self.date_widget = DateWidget(self, default_value=date.today())
        self.date_widget.grid(column=1, row=row, padx=5, pady=5, sticky=tk.W, columnspan=2)
        ttkb.Label(self, text="Time:", anchor=tk.E).grid(column=2, row=row, padx=5, pady=5)
        self.time_widget = TimeWidget(self)
        self.time_widget.grid(column=3, row=row, padx=5, pady=5, sticky=tk.W)
        row += 1
        for track_cfg in person.tracked.values():
            if track_cfg.tracked:
                widget = MetricWidgetFactory.make_widget(self, track_cfg)
                if widget:
                    if len(self.bio_entries) > 0:
                        prev_widget = self.bio_entries[-1: len(self.bio_entries)][0]
                        prev_widget.down_arrow_entry = widget
                        widget.up_arrow_entry = prev_widget
                    self.bio_entries.append(widget)
                widget.grid(column=0, row=row, sticky=tk.E, columnspan=4)
                row += 1
        row += 1
        ttkb.Button(self, text="Cancel", command=cancel_action).grid(column=0, row=row, sticky=NW, padx=10)
        ttkb.Button(self, text="Save", command=submit_action).grid(column=2, row=row, sticky=NW, padx=10)
        ttkb.Button(self, text="Save and Repeat",
                    command=lambda repeat=True: submit_action(repeat)).grid(column=3, row=row, sticky=NW, padx=10)
        self.date_widget.set_next_entry(self.time_widget)
        self.time_widget.set_prev_entry(self.date_widget)
        self.time_widget.set_next_entry(self.bio_entries[0])
        self.grid(padx=150)

    def clear_entries(self):
        for widget in self.bio_entries:
            widget.zero_value()
        self.time_widget.focus_set()


class CSVImportFrame(ttkb.Frame):
    """
    A ttkbootstrap Frame that allows the entry of the information necessary to import a CSV file
    """
    FIELD_NAMES: list[str] = [imp.DATE, imp.TIME, imp.BLOOD_PRESSURE, imp.BLOOD_GLUCOSE,
                              imp.BODY_WEIGHT, imp.BODY_TEMP, imp.PULSE, imp.NOTE, imp.IGNORE]

    def __init__(self, parent, person: dp.Person, cancel_action: Callable, proceed_action: Callable,
                 config_info: config.ConfigInfo):
        """
        Creates an instance of gui.CSVImportFrame

        :param parent: the GUI parent for this Frame
        :type parent: a Tk derived Window or Frame
        :param person: the Person object for the person whose data will be exported.
        :type person: biometrics_tracker.model.datapoints.Person
        :param cancel_action: a callback to be invoked when the cancel button is pressed.
        :type cancel_action: Callable
        :param proceed_action: a callback to be invoked when the proceed button is pressed.
        :type proceed_action: Callable

        """
        ttkb.Frame.__init__(self, parent)

        self.person = person
        self.config_info = config_info
        self.import_specs_collection: imp.ImportSpecsCollection = imp.ImportSpecsPickler(
            self.config_info.config_file_path.parent).retrieve()

        row = 0
        ttkb.Label(self, text="Import Data from CSV File").grid(column=0, row=row, columnspan=3, padx=5, pady=5)
        self.import_fn_var = ttkb.StringVar()
        row += 1

        ttkb.Label(self, text=f'Person: {self.person.id} {self.person.name} DOB: {self.person.dob.month}/'
                   f'{self.person.dob.day}/{self.person.dob.year}').grid(column=0, row=row,
                                                                         columnspan=5, padx=5, pady=5)
        row += 1

        ttkb.Label(self, text="Import File Name:", anchor=tk.E).grid(column=0, row=row, padx=5, pady=5)
        self.import_fn_entry = ttkb.Entry(self, textvariable=self.import_fn_var, width=40)
        self.import_fn_entry.grid(column=1, row=row, padx=5, pady=5, columnspan=2, sticky=tk.W)
        self.import_fn_entry.bind('<FocusOut>', self.update_preview)
        ttkb.Button(self, text="Browse...", command=self.browse_csv).grid(column=3, row=row, padx=5, pady=5, sticky=NW)
        row += 1
        self.header_rows_entry = widgets.LabeledIntegerWidget(self, label_text='Number of Header Rows:', label_width=25,
                                                              label_grid_args={'column': 0, 'row': row, 'padx': 5,
                                                                               'pady': 5}, entry_width=6,
                                                              entry_grid_args={'column': 1, 'row': row, 'padx': 5,
                                                                               'pady': 5})
        self.use_prev_row_date = False
        self.use_prev_row_date_var = ttkb.IntVar()
        self.use_prev_row_date_entry = Checkbutton(self,
                                                   text='Use date from previous row if date column is blank',
                                                   variable=self.use_prev_row_date_var,
                                                   command=self.set_use_prev_row_date, padding=5, width=50)
        self.use_prev_row_date_entry.grid(column=2, row=row, columnspan=3, padx=5, pady=5)
        self.widgets: list[ImportExportFieldWidget] = []
        row += 1
        ttkb.Label(self, text="Data Types for Each Column in the CSV File").grid(column=0, row=row, columnspan=2,
                                                                                 padx=5, pady=5)
        row += 1
        col = 0
        subframe = ttkb.Frame(self)
        subframe_row = 0
        for index, field_name in enumerate(CSVImportFrame.FIELD_NAMES):
            csv_widget = ImportExportFieldWidget(subframe, index, field_name, CSVImportFrame.FIELD_NAMES)
            csv_widget.grid(column=col, row=(index//3)+subframe_row, sticky=NSEW)
            self.widgets.append(csv_widget)
            if col == 0:
                col = 1
            elif col == 1:
                col = 2
            else:
                col = 0
        widget_rows = (len(CSVImportFrame.FIELD_NAMES)//3)
        subframe.grid(column=0, row=row, columnspan=3, rowspan=widget_rows, sticky=NW)

        self.header_rows_entry.bind('<FocusOut>', self.focus_first_widget)
        row += widget_rows + 1
        ttkb.Label(self, text="File Preview").grid(column=0, row=row, columnspan=3)
        row += 1
        self.preview: scrolled.ScrolledText = scrolled.ScrolledText(self, padding=5, height=9)
        self.preview.grid(column=0, row=row, padx=5, pady=5, sticky=NW, columnspan=4, rowspan=9)
        row += 9
        ttkb.Button(self, text='Cancel', command=cancel_action, width=10).grid(
            column=1, row=row, sticky=NW, padx=5, pady=5)
        ttkb.Button(self, text='Proceed', command=proceed_action, width=10).grid(column=2, row=row, sticky=NW,
                                                                                 padx=5, pady=5)
        self.import_spec_buttons: widgets.ImportExportSpecButtons = \
            widgets.ImportExportSpecButtons(parent=self, button_title='Import Specs',
                                            select_title='Select an Import Spec',
                                            select_action=self.select_import_spec,
                                            save_title='Save Import Spec',
                                            save_action=self.save_import_spec,
                                            specs_map=self.import_specs_collection)
        self.import_spec_buttons.grid(column=3, row=row, padx=5, pady=5, sticky=NW, columnspan=2)

        self.grid(padx=50, pady=5)

    def set_use_prev_row_date(self):
        """
        Set a boolean based on the value of the IntVar associated with the Use Previous Row Date checkbox

        :return: None
        """
        if self.use_prev_row_date_var.get() > 0:
            self.use_prev_row_date = True
        else:
            self.use_prev_row_date = False

    def browse_csv(self):
        """
        Presents a tk.filedialog to browse the file system and select a CSV file to be imported

        :return: None

        """
        filename = filedialog.askopenfilename(filetypes=[('CSV', '.csv')], initialdir='~',
                                              title="Select CSV File for Import")
        self.import_fn_var.set(filename)
        self.import_fn_entry.focus_set()

    def update_preview(self, event):
        """
        Update the ttkbootstrap.scrolled Scrollable Text widget to show the first 30 lines of the selected
        CSV files.  This method is bound the <FocusOut> event of the CSV file name entry

        :param event:
        :type event: Tkinter.Event
        :return: None
        """
        if len(self.import_file_name().strip()) > 0:
            try:
                with open(self.import_file_name(), mode='rt') as file:
                    for i in range(0, 30):
                        line = file.readline()
                        self.preview.insert(END, line)
                self.header_rows_entry.focus_set()
            except FileNotFoundError:
                dialogs.Messagebox.ok(f'File {self.import_file_name()} not found.')

    def import_file_name(self) -> str:
        """
        Returns the import file name

        :return: file name
        :rtype: str
        """
        return self.import_fn_var.get()

    def header_rows(self) -> int:
        """
        Returns the number of header rows in the CSV file to be imported

        :return: number of header rows
        :rtype: int
        """
        return self.header_rows_entry.get_value()

    def focus_first_widget(self, event):
        """
        Focuses on the first gui.widgets.CSVImportFrame.  This method is bound to the <FocusOut> widget of the
        'Number of Header Rows' entry field

        :param event:
        :type event: Tkinter.Event
        :return: None
        """
        self.widgets[0].focus_set()

    def indexed_fields(self) -> dict[int, str]:
        """
        Returns a dict[int, str] that has the position within the CSV file of the field as the key and the field name
        as the value.  The field names are taken from the string values defined at the beginning of the model.importers
        module.

        :return: map of fields selected for import
        :rtype: dict[int, str]
        """
        csv_fields: dict[int, str] = {}
        index = 1
        for widget in self.widgets:
            if widget.field_name() != imp.IGNORE:
                csv_fields[index] = widget.field_name()
                index += 1
        return csv_fields

    def select_import_spec(self, event):
        """
        This method is used as a callback for the selection action of the Select Import Spec dialog.
        It retrieves the selected specification from the ImportSpecificatonCollection and uses it
        to set the prompt values on this Frame

        :param event:
        :return: None
        """
        spec_id: Optional[str] = self.import_spec_buttons.get_selected_spec_id()
        self.import_spec_buttons.destroy_spec_list()
        if spec_id is not None:
            import_spec: imp.ImportSpec = self.import_specs_collection.get_spec(spec_id)
            self.header_rows_entry.set_value(import_spec.nbr_of_header_rows)
            self.use_prev_row_date_var.set(import_spec.use_date_from_prev_row)
            for widget in self.widgets:
                widget.field_name_var.set(import_spec.column_types[widget.index])

    def save_import_spec(self):
        """
        This method is used a callback for the OK button on the Save Import Spec dialog.  It collects the
        current values from the prompts, creates an ImportSpec object, places in an ImportSpecCollection
        object and pickles the collection object.

        :return: None

        """
        column_types: dict[int, str] = {}
        for widget in self.widgets:
            column_types[widget.index] = widget.field_name()

        import_spec: imp.ImportSpec = imp.ImportSpec(id=self.import_spec_buttons.get_saved_spec_id(),
                                                     nbr_of_header_rows=self.header_rows_entry.get_value(),
                                                     use_date_from_prev_row=self.use_prev_row_date,
                                                     column_types=column_types)
        self.import_specs_collection.add_spec(import_spec)
        pickler: imp.ImportSpecsPickler = imp.ImportSpecsPickler(self.config_info.config_file_path.parent)
        pickler.save(self.import_specs_collection)


class ImportExportStatusDialog(dialogs.MessageDialog):
    """
    A ttkbootstrap.dialogs Dialog that displays status info for a completed import/export run

    :return: None
    """
    def __init__(self, parent, msg: messages.ImportExportCompletionMsg, titles: list[str]):
        dialogs.MessageDialog.__init__(self, titles[0], parent=None,
                                       title=titles[1], buttons=['OK'], alert=True)
        self.msg: messages.ImportExportCompletionMsg = msg

    def create_body(self, parent):
        """
        Overrides the create_body method of the base class.  Builds the body of the Dialog

        :param parent: the GUI parent for this Dialog
        :type parent: Tkinter derived Window or Frame
        :return: None
        """
        if self.msg.status == messages.Completion.SUCCESS:
            ok = 'Successful'
        else:
            ok = 'Errors were encountered.'
        ttkb.Label(parent, text=f'Status: {ok}').pack(side=TOP)
        msg_count = self.msg.msg_count()
        msg_scroll = scrolled.ScrolledText(parent, padding=5, height=msg_count)
        for msg in self.msg.messages:
            msg_scroll.insert(END, f'{msg}\n')
        msg_scroll.pack(side=LEFT)

    def close_dialog(self):
        """
        Remove the Dialog from the screen.  This method is invoked when the close button is pressed.

        :return: None
        """
        self.destroy()


class ExportFrame(ttkb.Frame):
    """
    A ttkbootstrap Frame that allows the entry of the information necessary to export a CSV or
    SQLite database file
    """
    FIELD_NAMES: list[str] = [imp.DATE, imp.TIME, imp.BLOOD_PRESSURE, imp.BLOOD_GLUCOSE,
                              imp.BODY_WEIGHT, imp.BODY_TEMP, imp.PULSE, imp.NOTE, imp.IGNORE]

    def __init__(self, parent, person: dp.Person, cancel_action: Callable, proceed_action: Callable,
                 config_info: config.ConfigInfo):
        """
        Creates an instance of gui.CSVImportFrame

        :param parent: the GUI parent for this Frame
        :type parent: a Tk derived Window or Frame
        :param person: the Person object for the person whose data will be exported.
        :type person: biometrics_tracker.model.datapoints.Person
        :param cancel_action: a callback to be invoked when the cancel button is pressed.
        :type cancel_action: Callable
        :param proceed_action: a callback to be invoked when the proceed button is pressed.
        :type proceed_action: Callable
        :param config_info: configurations info, containing an biometrics_tracker.model.exporters.ExportSpecsCollection
        :type config_info: biometrics_tracker.config.createconfig.ConfigInfo
        """
        ttkb.Frame.__init__(self, parent)

        self.person = person
        self.cancel_action = cancel_action
        self.proceed_action = proceed_action
        self.config_info = config_info

        self.export_specs_collection: exp.ExportSpecsCollection = exp.ExportSpecsPickler(
            self.config_info.config_file_path.parent).retrieve()

        row = 0
        ttkb.Label(self, text="Export Data").grid(column=0, row=row, columnspan=3, padx=5, pady=5)
        self.export_dir_var = ttkb.StringVar()
        row += 1

        ttkb.Label(self, text=f'Person: {self.person.id} {self.person.name} DOB: {self.person.dob.month}/'
                   f'{self.person.dob.day}/{self.person.dob.year}').grid(column=0, row=row,
                                                                         columnspan=5, padx=5, pady=5)
        row += 1

        ttkb.Label(self, text=" Export File Directory:", width=25, anchor=tk.E).grid(column=0, row=row, padx=5, pady=5)
        self.export_dir_entry = ttkb.Entry(self, textvariable=self.export_dir_var, width=40)
        self.export_dir_entry.grid(column=1, row=row, padx=5, pady=5, columnspan=2, sticky=NW)
        ttkb.Button(self, text="Browse...", command=self.browse_export_dir).grid(column=3, row=row, padx=5, pady=5,
                                                                                 sticky=tk.W)

        row += 1
        ttkb.Label(self, text="Start Date:", width=25, anchor=tk.E).grid(column=0, row=row, padx=5, pady=5)
        self.start_date_widget = DateWidget(self, datetime.today())
        self.start_date_widget.grid(column=1, row=row, padx=5, pady=5, sticky=tk.W)
        ttkb.Label(self, text="End Date:", anchor=tk.E).grid(column=2, row=row, padx=5, pady=5)
        self.end_date_widget = DateWidget(self, datetime.today())
        self.end_date_widget.grid(column=3, row=row, padx=5, pady=5, sticky=tk.W)

        row += 1
        ttkb.Label(self, text="Export File Type:", width=25, anchor=tk.E).grid(column=0, row=row, padx=5, pady=5)
        self.export_type_var = ttkb.StringVar()
        self.export_type_var.set(exp.ExportType.CSV.name)
        self.export_type_entry = ttk.Combobox(self, textvariable=self.export_type_var,
                                              values=[exp.ExportType.CSV.name, exp.ExportType.SQL.name])
        self.export_type_entry.grid(column=1, row=row, padx=5, pady=5, sticky=tk.W)
        self.inc_header_var = ttkb.IntVar()
        self.inc_header_var.set(0)
        self.inc_header_entry = Checkbutton(self, text='Include a header row (CSV only)?', variable=self.inc_header_var,
                                            padding=5, width=35)
        self.inc_header_entry.grid(column=2, row=row, columnspan=2)
        row += 1

        ttkb.Label(self, text="Units of Measure will be:", width=25, anchor=tk.E).grid(column=0, row=row)
        self.uom_handling_var = ttkb.StringVar()
        self.uom_handling_var.set(exp.UOMHandlingType.Excluded.name)
        names: list[str] = [value.name for value in exp.UOMHandlingType]
        values: list[str] = []
        self.uom_word_value_map: dict[str, exp.UOMHandlingType] = {}
        for name in names:
            words = split_camelcase(name)
            value = ' '.join(words)
            values.append(value)
            self.uom_word_value_map[value] = exp.uom_handling_type_name_map[name]
        self.uom_handling_entry = ttk.Combobox(self, textvariable=self.uom_handling_var,
                                               values=values)
        self.uom_handling_entry.grid(column=1, row=row, padx=5, pady=5, sticky=NW)
        row += 1
        ttkb.Label(self, text="Date Format:", width=20, anchor=tk.E).grid(column=0, row=row, padx=5, pady=5)
        self.date_fmt_var = ttkb.StringVar()
        self.date_fmt_var.set('MM/DD/YYYY')
        self.date_fmt_entry = ttk.Combobox(self, textvariable=self.date_fmt_var,
                                           values=[f for f in exp.date_formats.keys()])
        self.date_fmt_entry.grid(column=1, row=row, padx=5, pady=5, sticky=tk.W)
        ttkb.Label(self, text="Time Format:", anchor=tk.E).grid(column=2, row=row, padx=5, pady=5)
        self.time_fmt_var = ttkb.StringVar()
        self.time_fmt_var.set('HH:MM:SS-24')
        self.time_fmt_entry = ttk.Combobox(self, textvariable=self.time_fmt_var,
                                           values=[f for f in exp.time_formats.keys()])
        self.time_fmt_entry.grid(column=3, row=row, padx=5, pady=5, sticky=tk.W)
        row += 1

        self.widgets: list[ImportExportFieldWidget] = []
        row += 1
        ttkb.Label(self, text="Data Types for Each Column in the Export File").grid(column=0, row=row, columnspan=2,
                                                                                    padx=5, pady=5)
        row += 1
        col = 0
        subframe = ttkb.Frame(self)
        subframe_row = 0
        for index, field_name in enumerate(CSVImportFrame.FIELD_NAMES):
            exp_widget = ImportExportFieldWidget(subframe, index, field_name, CSVImportFrame.FIELD_NAMES)
            exp_widget.grid(column=col, row=(index//3)+subframe_row, sticky=NSEW)
            self.widgets.append(exp_widget)
            if col == 0:
                col = 1
            elif col == 1:
                col = 2
            else:
                col = 0
        widget_rows = (len(ExportFrame.FIELD_NAMES)//3)
        subframe.grid(column=0, row=row, columnspan=3, rowspan=widget_rows, sticky=NW)

        row += widget_rows + 1
        ttkb.Button(self, text='Cancel', command=cancel_action).grid(
            column=1, row=row,  sticky=tk.E, padx=5, pady=5)
        ttkb.Button(self, text='Proceed', command=proceed_action).grid(column=2, row=row, sticky=tk.W, padx=5, pady=5)
        self.export_spec_buttons: widgets.ImportExportSpecButtons = \
            widgets.ImportExportSpecButtons(parent=self, button_title='Export Specs',
                                            select_title='Select an Export Spec',
                                            select_action=self.select_export_spec,
                                            save_title='Save Export Spec',
                                            save_action=self.save_export_spec,
                                            specs_map=self.export_specs_collection)
        self.export_spec_buttons.grid(column=3, row=row, padx=5, pady=5, columnspan=2, rowspan=2)
        row += 1

        self.start_date_widget.prev_entry = self.export_dir_entry
        self.start_date_widget.next_entry = self.end_date_widget
        self.end_date_widget.prev_entry = self.start_date_widget
        self.end_date_widget.next_entry = self.export_type_entry

        self.grid(padx=50, pady=5)

    def focus_set(self):
        """
        Delegates the focus_set method calls to the Export Directory entry field

        :return: None

        """
        self.export_dir_entry.focus_set()

    def browse_export_dir(self):
        """
        Presents a tk.filedialog to browse the file system and select a CSV file to be imported

        :return: None

        """
        initialdir = '~'
        if len(self.export_dir_entry.get().strip()) > 0:
            initialdir = self.export_dir_entry.get()
        directory = filedialog.askdirectory(initialdir=initialdir, mustexist=True,
                                            title="Select Directory where the Export File will be created" )
        self.export_dir_var.set(directory)
        self.export_dir_entry.focus_set()

    def get_export_dir_path(self) -> pathlib.Path:
        return pathlib.Path(self.export_dir_var.get())

    def get_start_date(self) -> date:
        return self.start_date_widget.get_date()

    def get_end_date(self) -> date:
        return self.end_date_widget.get_date()

    def get_export_type(self) -> exp.ExportType:
        return exp.export_type_name_map[self.export_type_var.get()]

    def get_include_header(self) -> bool:
        if self.inc_header_var.get() > 0:
            return True
        else:
            return False

    def get_uom_handling_type(self) -> exp.UOMHandlingType:
        return self.uom_word_value_map[self.uom_handling_var.get()]

    def get_date_format(self) -> str:
        return exp.date_formats[self.date_fmt_var.get()]

    def get_time_format(self) -> str:
        return exp.time_formats[self.time_fmt_var.get()]

    def indexed_fields(self) -> dict[str, int]:
        """
        Returns a dict[int, str] that has the field name as the key and the position within the Export file of the field
         as the value.  The field names are taken from the string values defined at the beginning of the
         model.importers module.

        :return: map of fields selected for export
        :rtype: dict[int, str]
        """
        export_fields: dict[str, int] = {}
        index = 0
        for widget in self.widgets:
            if widget.field_name() != imp.IGNORE:
                export_fields[widget.field_name()] = index
                if widget.field_name() in [exp.DATE, exp.TIME] or self.get_uom_handling_type() != \
                        exp.UOMHandlingType.InSeparateColumn:
                    index += 1
                else:
                    export_fields[f'{widget.field_name()}_UOM'] = index + 1
                    index += 2
        return export_fields

    def select_export_spec(self, event):
        spec_id: Optional[str] = self.export_spec_buttons.get_selected_spec_id()
        self.export_spec_buttons.destroy_spec_list()
        if spec_id is not None:
            export_spec: exp.ExportSpec = self.export_specs_collection.get_spec(spec_id)
            self.export_type_var.set(export_spec.export_type.name)
            if export_spec.include_header_row:
                self.inc_header_var.set(1)
            else:
                self.inc_header_var.set(0)
            self.uom_handling_var.set(export_spec.uom_handling_type.name)
            self.date_fmt_var.set(export_spec.date_format)
            self.time_fmt_var.set(export_spec.time_format)
            for widget in self.widgets:
                widget.field_name_var.set(export_spec.column_types[widget.index])
            self.update()

    def save_export_spec(self):
        """
        This method is used as a callback for the Save Export Spec dialog.  It collects the values of the
        entry prompts in the Frame, creates an instance of ExportSpec, places that instance in an instance
        of ExportSpecCollection and pickles the collection object

        :return: None
        """
        column_types: dict[int, str] = {}
        for widget in self.widgets:
            column_types[widget.index] = widget.field_name()

        export_spec: exp.ExportSpec = exp.ExportSpec(id=self.export_spec_buttons.get_saved_spec_id(),
                                                     export_type=self.get_export_type(),
                                                     include_header_row=self.get_include_header(),
                                                     uom_handling_type=self.get_uom_handling_type(),
                                                     date_format=exp.date_format_strft_map[self.get_date_format()],
                                                     time_format=exp.time_format_strft_map[self.get_time_format()],
                                                     column_types=column_types)
        self.export_specs_collection.add_spec(export_spec)
        pickler: exp.ExportSpecsPickler = exp.ExportSpecsPickler(self.config_info.config_file_path.parent)
        pickler.save(self.export_specs_collection)


class BiometricsFrameBase(ttkb.Frame):
    """
    This is a base class for frames that display biometrics data in various ways
    """
    def __init__(self, title: str,  parent, person: dp.Person, queue_mgr: queues.Queues):
        """
        Create an instance for BiometricsFrameBase

        :param title: a title to be displayed at the top of the Frame
        :type title: str
        :param parent: the GUI parent for this frame
        :type parent: Tkinter derived Window or Frame
        :param person: info for the person whose history is to be displayed
        :type person: biometrics_tracker.model.datapoints.Person
        :param queue_mgr: message queue manager
        :type queue_mgr: biometrics_tracker.ipc.queue_mgr.Queues
        """
        ttkb.Frame.__init__(self, parent)
        self.person = person
        self.queue_mgr = queue_mgr
        self.start_date: Optional[datetime] = None
        self.end_date: Optional[datetime] = None
        self.all_widgets: list[dp_widget_union] = []
        self.hdr_frame = ttkb.Frame(self)
        self.start_date_widget = DateWidget(self.hdr_frame)
        self.end_date_widget = DateWidget(self.hdr_frame, datetime.today())
        self.retrieve_btn = ttkb.Button(self.hdr_frame, text='Retrieve History', command=self.retrieve_history)
        self.row = 0
        ttkb.Label(self.hdr_frame, text=title).grid(column=0, row=self.row, columnspan=4, padx=5, pady=5)
        self.row += 1
        ttkb.Label(self.hdr_frame, text=f'Person: {self.person.id} {self.person.name} DOB: {self.person.dob.month}/'
                   f'{self.person.dob.day}/{self.person.dob.year}').grid(column=0, row=self.row,
                                                                         columnspan=5, padx=5, pady=5)
        self.row += 1
        ttkb.Label(self.hdr_frame, text="Start Date:", width=15, anchor=tk.E).grid(column=0, row=self.row, padx=5, pady=5)
        self.start_date_widget.grid(column=1, row=self.row, padx=5, pady=5, sticky=tk.W)
        ttkb.Label(self.hdr_frame, text="End Date:").grid(column=2, row=self.row, padx=5, pady=5)
        self.end_date_widget.grid(column=3, row=self.row, padx=5, pady=5)
        self.retrieve_btn.grid(column=4, row=self.row, padx=5, pady=5)
        self.row += 1
        self.start_date_widget.set_next_entry(self.end_date_widget)
        self.end_date_widget.set_prev_entry(self.start_date_widget)
        self.end_date_widget.set_next_entry(self.retrieve_btn)

    def focus_set(self):
        """
        Delegate focus_set calls to the start date widget

        :return: None
        """
        self.start_date_widget.focus_set()

    @abstractmethod
    def retrieve_history(self):
        """
        This method must be overridden by inheriting classes to retrieve Datapoints for a specified prerson within a
        specified date range.

        :return: None
        """
        ...


class ViewEditBiometricsFrame(BiometricsFrameBase):
    """
    A Frame that allows the edit and deletion of DataPoints

    """
    def __init__(self, parent, person: dp.Person, queue_mgr: queues.Queues, cancel_action: Callable,
                 submit_action: Callable):
        """
        Create an instance of ViewEditBiometricsFrame

        :param parent: the GUI parent for this frame
        :type parent: Tk derived Window or Frame
        :param person: the info for the person whose history is to be displayed
        :type person: biometrics_tracker.model.datapoints.Person
        :param queue_mgr: message queue manager
        :type queue_mgr: biometrics_tracker.ipc.queue_mgr.Queues
        :param cancel_action: a callback that will be invoked when the Cancel button is pressed
        :type cancel_action: Callable
        :param submit_action: a callback that will be invoked when the Save button is pressed
        :type submit_action: Callable
        """
        BiometricsFrameBase.__init__(self, 'View/Edit Biometrics History', parent, person, queue_mgr)
        self.parent: Application = parent
        self.submit_action: Callable = submit_action
        self.hdr_frame.grid(column=0, row=0, padx=5, pady=5)
        self.entry_frame = ttkb.Frame(self)
        self.datapoint_widget_row: int = 0
        self.datapoint_widget_col: int = 0
        self.datapoint_widget: Optional[Union[widgets.DataPointWidget, widgets.PlaceholderWidget]] = \
            self.create_placeholder_widget()
        self.datapoint_widget.grid(column=self.datapoint_widget_col, row=self.datapoint_widget_row,
                                   rowspan=5, columnspan=4)
        cancel = ttkb.Button(self.entry_frame, text='Cancel', command=cancel_action)
        cancel.grid(column=3, row=7, padx=5, pady=5)
        save = ttkb.Button(self.entry_frame, text='Save', command=self.save_datapoint)
        save.grid(column=4, row=7, padx=5, pady=5)
        self.entry_frame.grid(column=0, row=1, padx=5, pady=5)
        self.list_frame = scrolled.ScrolledFrame(self, bootstyle='darkly', height=300, width=770)
        self.list_frame.grid(column=0, row=7, padx=5, pady=5)
        self.grid(sticky=NSEW)
        self.payload_re = re.compile('!payload\\w*')

        row: int = 13
        self.grid(padx=60)
        self.start_date_widget.set_next_entry(self.end_date_widget)
        self.end_date_widget.set_prev_entry(self.start_date_widget)
        self.end_date_widget.set_next_entry(self.retrieve_btn)
        self.delete_boxes = []
        self.current_payload_label: Optional[widgets.PayloadLabel] = None

    def create_placeholder_widget(self) -> widgets.PlaceholderWidget:
        """
        Create a placeholder widget to take up space until the user selects a DataPoint to edit

        :return: None

        """
        return widgets.PlaceholderWidget(self.entry_frame, rows=5, width=30)

    def retrieve_history(self):
        """
        Retrieve the datapoints within the specified date range for the selected person and fill a ScrollableFrame
        with rows of widgets

        :return: None
        """
        self.start_date = self.start_date_widget.get_date()
        self.end_date = self.end_date_widget.get_date()

        if self.start_date > self.end_date:
            dialogs.Messagebox.ok('The end date must be on or after the start date', 'Date Entry Error')
            self.start_date_widget.focus_set()
            return
        start_datetime = mk_datetime(self.start_date, time(hour=0, minute=0, second=0))
        end_datetime = mk_datetime(self.end_date, time(hour=23, minute=59, second=59))
        self.parent.retrieve_datapoints(replyto=self.display_history, person_id=self.person.id,
                                        start_datetime=start_datetime, end_datetime=end_datetime)

    def display_history(self, msg: messages.DataPointRespMsg):
        """
        Create a entry prompts from the DataPoints history retrieved by the retrieve_history method

        :param msg: a response message containing a list of DataPoints
        :return: None

        """
        datapoints: list[dp.dptypes_union] = msg.datapoints
        for child in self.list_frame.children:
            if self.payload_re.match(child):
                widget = self.list_frame.nametowidget(child)
                widget.grid_remove()
        row: int = 0
        for datapoint in datapoints:
            delete_var = ttkb.IntVar()
            delete_var.set(0)
            delete_box = widgets.PayloadCheckbutton(self.list_frame, text='Del', variable=delete_var,
                                                    payload=(datapoint.taken, datapoint.type))
            self.delete_boxes.append((delete_var, datapoint.taken, datapoint.type))
            delete_box.grid(column=0, row=row, sticky=NW, padx=5, pady=5)
            label = widgets.PayloadLabel(self.list_frame, text=datapoint.__str__(), width=400, payload=datapoint)
            label.grid(column=1, row=row, sticky=NW, padx=5, pady=5)
            label.bind('<Double-1>', self.edit_datapoint)
            row += 1
        self.list_frame.grid()
        self.start_date_widget.focus_set()

    def edit_datapoint(self, event):
        """
        A callback method to be invoked on the double click of a PayloadLabel entry in the list of retrieved
        DataPoints

        :param event: the event generated by the double click
        :type event: tkinter.Event
        :return: None

        """
        self.current_payload_label = event.widget
        for child in self.list_frame.children:
            if self.payload_re.match(child):
                widget = self.list_frame.nametowidget(child)
                if isinstance(widget, widgets.PayloadLabel):
                    widget.config(background='black')
        self.current_payload_label.config(background='blue')
        datapoint = event.widget.payload
        track_cfg = self.person.dp_type_track_cfg(datapoint.type)
        self.datapoint_widget.forget()
        self.datapoint_widget = widgets.DataPointWidget(self.entry_frame, track_cfg, datapoint)
        self.datapoint_widget.grid(column=self.datapoint_widget_col, row=self.datapoint_widget_row, sticky=tk.NW,
                                   padx=5, pady=5, columnspan=4)
        self.datapoint_widget.focus_set()

    def save_datapoint(self):
        """
        A callback method that handles deletes and updates when the Save button is clicked

        """
        deleted_list: list[tuple[datetime, dp.DataPointType]] = []
        for delete_var, taken, dp_type in self.delete_boxes:
            if delete_var.get() > 0:
                self.queue_mgr.send_db_req_msg(messages.DataPointReqMsg(destination=per.DataBase, replyto=None,
                                                                        person_id=self.person.id, start=taken,
                                                                        end=taken, dp_type=dp_type,
                                                                        operation=messages.DBOperation.DELETE_SINGLE))
                deleted_list.append((taken, dp_type))

        key_change: bool = False
        if self.datapoint_widget.is_changed():
            key_timestamp: datetime = self.current_payload_label.payload.taken
            datapoint: dp.DataPoint = self.datapoint_widget.get_datapoint()
            key_change = not compare_mdyhms(key_timestamp, datapoint.taken)
            if not key_change:
                self.current_payload_label['text'] = datapoint.__str__()
                self.current_payload_label['background'] = ''
                self.current_payload_label.payload = datapoint
            self.parent.save_datapoint(self.person, datapoint.taken, widget=self.datapoint_widget.get_metric_widget(),
                                       delete_if_zero=True, db_operation=messages.DBOperation.UPDATE,
                                       key_timestamp=key_timestamp)
        if len(deleted_list) > 0 and not key_change:
            for child in self.list_frame.children:
                if self.payload_re.match(child):
                    widget = self.list_frame.nametowidget(child)
                    payload = widget.payload
                    match widget.__class__:
                        case widgets.PayloadCheckbutton:
                            if (payload[0], payload[1]) in deleted_list:
                                widget.grid_remove()
                        case widgets.PayloadLabel:
                            if (payload.taken, payload.type) in deleted_list:
                                widget.grid_remove()

        self.datapoint_widget.grid_remove()
        self.datapoint_widget = self.create_placeholder_widget()
        self.datapoint_widget.grid(column=self.datapoint_widget_col, row=self.datapoint_widget_row,
                                   rowspan=5, columnspan=3)
        if key_change:
            self.retrieve_history()


class BiometricsReportPromptFrame(BiometricsFrameBase):
    def __init__(self, parent, person: dp.Person, queue_mgr: queues.Queues, close_action: Callable):
        """
        Create an instance of ViewEditBiometricsFrame

        :param parent: the GUI parent for this frame
        :type parent: Tk derived Window or Frame
        :param person: the dp.Person instance for the person whose history is to be displayed
        :type person:  biometrics_tracker.model.datapoints.Person
        :param queue_mgr: message queue manager
        :type queue_mgr: biometrics_tracker.ipc.queue_mgr.Queues
        :param close_action: a callback that will be invoked when the Close button is pressed
        :type close_action: Callable
        """
        BiometricsFrameBase.__init__(self, 'Biometrics History Report', parent, person, queue_mgr)

        ttkb.Button(self.hdr_frame, text="Close", command=close_action).grid(column=4, row=self.row, padx=5, pady=5)
        self.hdr_frame.grid(column=0, row=0)
        self.rpt_frame = None
        self.col_dp_map: dict[dp.DataPointType, int] = {}
        for idx, track_cfg in enumerate(self.person.tracked.values()):
            self.col_dp_map[track_cfg.dp_type] = idx + 2
        self.rpt_text = None
        self.rptf = None
        self.save_rpt_frame = None
        self.grid(sticky=NW)

    def retrieve_history(self):
        """
        Retrieve the datapoints within the specified date range for the selected person and produce a text report

        :return: None
        """
        self.start_date = self.start_date_widget.get_date()
        self.end_date = self.end_date_widget.get_date()
        if self.start_date > self.end_date:
            dialogs.Messagebox.ok('The end date must be on or after the start date', 'Date Entry Error')
            self.start_date_widget.focus_set()
            return
        self.rptf: StringIO = StringIO()
        rpt: reports.TextReport = reports.TextReport(self.queue_mgr, self.person, self.start_date, self.end_date,
                                                     self.rptf, completion_destination=self.display_history)
        rpt.start()

    def display_history(self, msg: messages.CompletionMsg):
        """
        Display the DataPoints retrieved by the retrieve_history method in a ScrolledText widget.  This content
        is also used to create the Text version of the History Report

        :param msg: a completion message notifying that the report creation process is complete
        :type msg: biometrics_tracker.ipc.messges.CompletionMsg
        :return:
        """
        self.start_date_widget.disable()
        self.end_date_widget.disable()
        self.retrieve_btn.configure(state=DISABLED)
        self.rpt_text = scrolled.ScrolledText(self, padding=5, hbar=True, height=30, width=105)
        self.rpt_text.insert(END, self.rptf.getvalue())
        self.rpt_text.grid(column=0, row=1)
        self.save_rpt_frame = SaveBiometricsReportFrame(self)
        self.save_rpt_frame.grid(column=0, row=2)


class SaveBiometricsReportFrame(ttkb.Frame):
    """
    Presents a ttkb.Frame that displays a Biometrics History Report and allows the user to save the report as a text
    or PDF file and optionally email the report to one or more recipients
    """
    TEXT: str = 'Text'
    PDF: str = 'PDF'
    DONT_SAVE: str = "Don't Save"

    def __init__(self, parent: BiometricsReportPromptFrame):
        """
        Create an instance of SaveBiometricsReportFrame

        :param parent: the GUI parent for this frame
        :type parent: Tk derived Window or Frame
        """
        ttkb.Frame.__init__(self, parent)
        self.parent = parent
        ttkb.Label(self, text='Save Report As:').grid(column=0, row=0, padx=8)
        self.save_rpt_var = ttkb.StringVar()
        ttkb.Radiobutton(self, text=SaveBiometricsReportFrame.TEXT, variable=self.save_rpt_var,
                         value=SaveBiometricsReportFrame.TEXT, command=self.enable_save).grid(column=1, row=0, padx=8)
        ttkb.Radiobutton(self, text=SaveBiometricsReportFrame.PDF, variable=self.save_rpt_var,
                         value=SaveBiometricsReportFrame.PDF, command=self.enable_save).grid(column=2, row=0, padx=8)
        ttkb.Radiobutton(self, text=SaveBiometricsReportFrame.DONT_SAVE, variable=self.save_rpt_var,
                         value=SaveBiometricsReportFrame.DONT_SAVE, command=self.disable_save).grid(column=3, row=0,
                                                                                                    padx=8)
        self.save_rpt_var.set(SaveBiometricsReportFrame.DONT_SAVE)
        self.file_name_var = ttkb.StringVar()
        self.save_button = ttkb.Button(self, text="Save",
                                       command=lambda text=parent.rptf, prt=False: self.save_report(text, prt))
        self.save_button.grid(column=5, row=0, padx=5, pady=5)
        self.save_button.config(state=DISABLED)
        self.save_print_button = ttkb.Button(self, text="Save and Print",
                                             command=lambda text=parent.rptf, prt=True: self.save_report(text, prt))
        self.save_print_button.grid(column=6, row=0, padx=5, pady=5)
        self.save_print_button.config(state=DISABLED)
        self.save_file_name = None

    def enable_save(self):
        """
        Enables the save button

        :return: None
        """
        self.save_button.config(state=tk.NORMAL)
        self.save_print_button.config(state=tk.NORMAL)

    def disable_save(self):
        """
        Disables the save button

        :return: None
        """
        self.save_button.config(state=DISABLED)

    def save_report(self, text: StringIO, prt: bool):
        """
        Save the report to a file

        :param text: the destination for the save report
        :type text: descendant of IOBase
        :param prt: did user request print
        :type prt: bool
        :return: None
        """
        self.save_file_name = filedialog.asksaveasfilename(confirmoverwrite=True)
        if isinstance(self.save_file_name, tuple):
            self.save_file_name = None
        else:
            text.seek(0, SEEK_SET)
            match self.save_rpt_var.get():
                case SaveBiometricsReportFrame.TEXT:
                    with pathlib.Path(self.save_file_name).open(mode='tw', encoding='UTF-8') as rpt_file:
                        rpt_file.write(text.read())
                    if prt:
                        self.show_printer_dialog()
                case SaveBiometricsReportFrame.PDF:
                    start_date = self.parent.start_date_widget.get_date()
                    end_date = self.parent.end_date_widget.get_date()
                    comp_dest: Callable = self.no_print
                    if prt:
                        comp_dest = self.show_printer_dialog
                    pdf_rpt: reports.PDFReport = reports.PDFReport(self.parent.queue_mgr, self.parent.person, start_date,
                                                                   end_date, self.save_file_name,
                                                                   completion_destination=comp_dest)
                    pdf_rpt.start()
                case _:
                    pass

    def show_printer_dialog(self, msg: Optional[messages.CompletionMsg] = None):
        """
        Open the report file in a browser so it can be printed via the browser print dialog

        :param msg: report completion message
        :type msg: biometrics_tracker.ipc.messages.CompletionMsg
        :return: None

        """
        webbrowser.open_new_tab(self.save_file_name)

    def no_print(self, msg: Optional[messages.CompletionMsg] = None):
        """
        This method is invoked in response to the report completion message if the user did not request printing

        :param msg: report completion message
        :type msg: biometrics_tracker.ipc.messages.CompletionMsg
        :return: None

        """
        pass


class AddEditScheduleFrame(ttkb.Frame):
    """
    Implements a GUI to facilitate the maintenance of ScheduleEntry's
    """
    def __init__(self, parent, person: dp.Person, queue_mgr: queues.Queues, cancel_action: Callable,
                 submit_action: Callable):
        """
        Creates an instance of AddEditScheduleFrame

        :param parent: the GUI parent of the frame
        :param person: a Person instance representing the person whose schedules will be added/edited
        :type person: biometrics_tracking.model.datapoints.Person
        :param queue_mgr: a Queues instance to provide access to the inter-thread message queues
        :type queue_mgr: biometrics_tracker.ipc.queues.Queues
        :param cancel_action: the method that will be invoked when the Cancel button is clicked
        :type cancel_action: Callable
        :param submit_action: the methold that will be invoked when the Save button is clicked
        :type submit_action: Callable

        :TODO add ability to clear the Last Triggered datetime property so that a schedule entry can be reprocessed

        """
        ttkb.Frame.__init__(self, parent)
        self.person: dp.Person = person
        self.queue_mgr = queue_mgr
        self.cancel_action: Callable = cancel_action
        self.submit_action: Callable = submit_action
        self.schedule_entry: Optional[dp.ScheduleEntry] = None
        self.max_seq_nbr: int = 0
        self.delete_boxes: list[(ttkb.IntVar, str, int)] = []
        self.db_operation: messages.DBOperation = messages.DBOperation.INSERT
        self.payload_re = re.compile('!payload\w*')
        self.entry_frame = ttkb.Frame(self)
        row: int = 0
        ttkb.Label(self.entry_frame, text="Add or Edit Schedules").grid(column=0, row=row, columnspan=4)
        row += 1
        ttkb.Label(self.entry_frame, text=f'Person: {self.person.id} {self.person.name} DOB: {self.person.dob.month}/'
                                          f'{self.person.dob.day}/{self.person.dob.year}').grid(column=0, row=row,
                                                                                                columnspan=5, padx=5,
                                                                                                pady=5)
        row += 1
        ttkb.Label(self.entry_frame, text='Start Date:').grid(column=0, row=row, sticky=NW, padx=5, pady=5)
        self.start_date_entry: DateWidget = DateWidget(self.entry_frame, default_value=date.today())
        self.start_date_entry.grid(column=1, row=row, sticky=tk.W, padx=5, pady=5)
        ttkb.Label(self.entry_frame, text='End Date:').grid(column=2, row=row, sticky=NW, padx=5, pady=5)
        self.end_date_entry: DateWidget = DateWidget(self.entry_frame, default_value=date.today())
        self.end_date_entry.grid(column=3, row=row, sticky=tk.W, padx=5, pady=5)
        ttk.Label(self.entry_frame, text='Data Point Type:').grid(column=4, row=row, sticky=NW, padx=5, pady=5)
        self.dp_type_entry = widgets.DataPointTypeComboWidget(self.entry_frame, self.person)
        self.dp_type_entry.grid(column=5, row=row, sticky=NW)
        row += 1
        self.seq_nbr_entry = widgets.LabeledIntegerWidget(parent=self.entry_frame, label_text='Seq Nbr:', label_width=10,
                                                          label_grid_args={'column': 0, 'row': row, 'padx': 5,
                                                                           'pady': 5}, entry_width=8,
                                                          entry_grid_args={'column': 1, 'row': row, 'sticky': tk.NW,
                                                                           'padx': 5, 'pady': 5})
        self.seq_nbr_entry.bind('<FocusIn>', self.focus_on_note)

        ttkb.Label(self.entry_frame, text='Note:').grid(column=2, row=row, sticky=NW, padx=5, pady=5)
        self.note_var = ttkb.StringVar()
        self.note_entry = ttkb.Entry(self.entry_frame, textvariable=self.note_var, width=50)
        self.note_entry.grid(column=3, row=row, sticky=NW, columnspan=4, padx=5, pady=5)

        row += 1
        ttkb.Label(self.entry_frame, text='Last Triggered:').grid(column=0, row=row, sticky=NW)
        self.last_triggered_var = ttkb.StringVar()
        last_trig = ttkb.Entry(self.entry_frame, textvariable=self.last_triggered_var)
        last_trig.grid(column=1, row=row, sticky=tk.W, padx=5, pady=5)
        last_trig.bind('<FocusIn>', self.focus_on_note)
        if self.schedule_entry is not None and self.schedule_entry.last_triggered is not None:
            self.last_triggered_var.set(self.schedule_entry.last_triggered.strftime('%m/%d/%Y %I:%M:%S %p'))

        row += 1
        column: int = 0
        self.frequency_var = ttkb.StringVar()
        button_width = 10
        button_frame = ttkb.Frame(self.entry_frame)
        hourly_button = Radiobutton(button_frame, text=dp.FrequencyType.Hourly.name, command=self.hourly_selected,
                                    value=dp.FrequencyType.Hourly.name, variable=self.frequency_var, width=button_width)
        hourly_button.grid(column=column, row=0, sticky=NW, padx=5, pady=5)
        column += 1
        daily_button = Radiobutton(button_frame, text=dp.FrequencyType.Daily.name, command=self.daily_selected,
                                   value=dp.FrequencyType.Daily.name, variable=self.frequency_var, width=button_width)

        daily_button.grid(column=column, row=0, sticky=NW, padx=5, pady=5)
        column += 1
        weekly_button = Radiobutton(button_frame, text=dp.FrequencyType.Weekly.name, command=self.weekly_selected,
                                    value=dp.FrequencyType.Weekly.name, variable=self.frequency_var,
                                    width=button_width)
        weekly_button.grid(column=column, row=0, sticky=NW, padx=5, pady=5)
        column += 1

        monthly_button = Radiobutton(button_frame, text=dp.FrequencyType.Monthly.name,
                                     command=self.monthly_selected,
                                     value=dp.FrequencyType.Monthly.name, variable=self.frequency_var,
                                     width=button_width)
        monthly_button.grid(column=column, row=0, sticky=NW, padx=5, pady=5)
        column += 1

        one_time_button = Radiobutton(button_frame, text=dp.FrequencyType.One_Time.name, command=self.once_selected,
                                      value=dp.FrequencyType.One_Time.name, variable=self.frequency_var,
                                      width=button_width)
        one_time_button.grid(column=column, row=0, sticky=NW, padx=5, pady=5)

        column += 1
        self.suspended_var = ttkb.IntVar()
        self.suspended_entry = Checkbutton(button_frame, text="Suspended", variable=self.suspended_var)
        self.suspended_entry.grid(column=column, row=0, stick=NW, padx=5, pady=5)
        button_frame.grid(column=0, row=row, columnspan=5)

        row += 1
        self.schedule_widget_row: int = row
        self.schedule_widget_col: int = 3
        self.schedule_widget: Optional[Union[widgets.ScheduleWidget, widgets.PlaceholderWidget]] = \
            widgets.PlaceholderWidget(self.entry_frame, rows=10, width=30)
        self.schedule_widget.grid(column=self.schedule_widget_col, row=self.schedule_widget_row, rowspan=5,
                                  columnspan=4)

        row += 10
        cancel = ttkb.Button(self.entry_frame, text='Cancel', command=cancel_action)
        cancel.grid(column=3, row=row, padx=5, pady=5)
        save = ttkb.Button(self.entry_frame, text='Save', command=self.validate)
        save.grid(column=4, row=row, padx=5, pady=5)
        self.entry_frame.grid(column=0, row=0, padx=5, pady=5)
        self.list_frame = scrolled.ScrolledFrame(self, bootstyle='darkly', height=150, width=770)
        self.list_frame.grid(column=0, row=1, padx=5, pady=5)
        self.grid(padx=60)
        self.start_date_entry.set_next_entry(self.end_date_entry)
        self.end_date_entry.set_prev_entry(self.start_date_entry)
        self.end_date_entry.set_next_entry(self.dp_type_entry)

        self.retrieve_schedules()

    def focus_on_note(self, event):
        """
        Forces the focus to the note field.  This method is used to prevent the Seq No field, which is intended to be
        display only, from gaining focus

        :param event:
        :return: None

        """
        if event.keysym == 'BackTab':
            self.dp_type_entry.focus_set()
        else:
            self.note_entry.focus_set()

    def focus_set(self):
        """
        Delagates the focus_set invocations to the Starts On date entry widget

        :return: None

        """
        self.start_date_entry.focus_set()

    def clear_entry(self):
        """
        Clears the entry widgets except for the Starts On and Ends On date entry widgets, and removes any
        schedule widget that is present.

        :return: None

        """
        self.dp_type_entry.selection_clear()
        self.dp_type_entry.dp_type_var.set('')
        self.seq_nbr_entry.set_value(0)
        self.note_var.set('')
        self.last_triggered_var.set('')
        self.frequency_var.set(value='')
        if self.schedule_widget is not None:
            self.schedule_widget.forget()
            self.schedule_widget.destroy()
            self.schedule_widget = None
        self.grid()
        self.db_operation = messages.DBOperation.INSERT

    def get_non_entered_values(self) -> (int, int, time, Optional[date]):
        """
        Provides values for info that may not (yet) be entered by the user

        :return: tuple[int, int, time, Optional[date])

        """
        seq_nbr: int = self.max_seq_nbr + 1
        interval: int = 1
        when_time: Optional[time] = None
        last_triggered: Optional[datetime] = None
        if self.schedule_entry is not None:
            seq_nbr = self.schedule_entry.seq_nbr
            interval = self.schedule_entry.interval
            when_time = self.schedule_entry.when_time
            last_triggered = self.schedule_entry.last_triggered
        return seq_nbr, interval, when_time, last_triggered

    def hourly_selected(self):
        """
        Instantiate an Hourly Schedule widget and grid it into the display

        :return: None

        """
        if self.schedule_widget is not None:
            self.schedule_widget.destroy()
        seq_nbr, interval, when_time, last_triggered = self.get_non_entered_values()
        suspended: bool = False
        if self.suspended_var.get() != 0:
            suspended = True
        self.schedule_widget = widgets.HourlyScheduleWidget(self.entry_frame, person_id=self.person.id,
                                                            seq_nbr=seq_nbr,
                                                            dp_type=self.dp_type_entry.get_dp_type(),
                                                            starts_on=self.start_date_entry.get_date(),
                                                            ends_on=self.end_date_entry.get_date(),
                                                            note=self.note_var.get(),
                                                            interval=interval, when_time=when_time,
                                                            suspended=suspended,
                                                            last_triggered=last_triggered)
        self.schedule_widget.grid(column=self.schedule_widget_col, row=self.schedule_widget_row)
        self.schedule_widget.focus_set()

    def daily_selected(self):
        """
        Instantiate a Daily Schedule widget and grid it into the display

        :return: None

        """
        if self.schedule_widget is not None:
            self.schedule_widget.destroy()
        seq_nbr, interval, when_time, last_triggered = self.get_non_entered_values()
        suspended: bool = False
        if self.suspended_var.get() != 0:
            suspended = True
        self.schedule_widget = widgets.DailyScheduleWidget(self.entry_frame, person_id=self.person.id,
                                                           seq_nbr=seq_nbr,
                                                           dp_type=self.dp_type_entry.get_dp_type(),
                                                           starts_on=self.start_date_entry.get_date(),
                                                           ends_on=self.end_date_entry.get_date(),
                                                           note=self.note_var.get(), interval=interval,
                                                           when_time=when_time, suspended=suspended,
                                                           last_triggered=last_triggered)
        self.schedule_widget.grid(column=self.schedule_widget_col, row=self.schedule_widget_row)
        self.schedule_widget.focus_set()

    def weekly_selected(self):
        """
        Instantiate a Weekly Schedule widget and grid it into the diplay

        :return: None

        """
        if self.schedule_widget is not None:
            self.schedule_widget.destroy()
        seq_nbr, interval, when_time, last_triggered = self.get_non_entered_values()
        weekdays: list[dp.WeekDay] = []
        if self.schedule_entry is not None:
            weekdays = self.schedule_entry.weekdays
        suspended: bool = False
        if self.suspended_var.get() != 0:
            suspended = True
        self.schedule_widget = widgets.WeeklyScheduleWidget(self.entry_frame, person_id=self.person.id,
                                                            seq_nbr=seq_nbr,
                                                            dp_type=self.dp_type_entry.get_dp_type(),
                                                            starts_on=self.start_date_entry.get_date(),
                                                            ends_on=self.end_date_entry.get_date(),
                                                            note=self.note_var.get(), weekdays=weekdays,
                                                            interval=interval, when_time=when_time,
                                                            suspended=suspended, last_triggered=last_triggered)
        self.schedule_widget.grid(column=self.schedule_widget_col, row=self.schedule_widget_row)
        self.schedule_widget.focus_set()

    def monthly_selected(self):
        """
        Instantiate a Monthly Schedule widget and grid it into the display

        :return: None

        """
        if self.schedule_widget is not None:
            self.schedule_widget.destroy()
        seq_nbr, interval, when_time, last_triggered = self.get_non_entered_values()
        days_of_month: list[int] = []
        if self.schedule_entry is not None:
            days_of_month = self.schedule_entry.days_of_month
        suspended: bool = False
        if self.suspended_var.get() != 0:
            suspended = True
        self.schedule_widget = widgets.MonthlyScheduleWidget(self.entry_frame, person_id=self.person.id,
                                                             seq_nbr=seq_nbr,
                                                             dp_type=self.dp_type_entry.get_dp_type(),
                                                             starts_on=self.start_date_entry.get_date(),
                                                             ends_on=self.end_date_entry.get_date(),
                                                             note=self.note_var.get(), days_of_month=days_of_month,
                                                             interval=interval, when_time=when_time,
                                                             suspended=suspended, last_triggered=last_triggered)
        self.schedule_widget.grid(column=self.schedule_widget_col, row=self.schedule_widget_row)
        self.schedule_widget.focus_set()

    def once_selected(self):
        """
        Instantiate a One Time Schedule widget and grid it into the display

        :return: None

        """
        if self.schedule_widget is not None:
            self.schedule_widget.destroy()
        seq_nbr, interval, when_time, last_triggered = self.get_non_entered_values()
        suspended: bool = False
        if self.suspended_var.get() != 0:
            suspended = True
        self.schedule_widget = widgets.OneTimeScheduleWidget(self.entry_frame, person_id=self.person.id,
                                                             seq_nbr=seq_nbr,
                                                             dp_type=self.dp_type_entry.get_dp_type(),
                                                             starts_on=self.start_date_entry.get_date(),
                                                             ends_on=self.end_date_entry.get_date(),
                                                             note=self.note_var.get(), interval=interval,
                                                             when_time=when_time, suspended=suspended,
                                                             last_triggered=last_triggered)
        self.schedule_widget.grid(column=self.schedule_widget_col, row=self.schedule_widget_row)
        self.schedule_widget.focus_set()

    def retrieve_schedules(self, msg: Optional[messages.ScheduleEntriesMsg] = None):
        """
        Issue a ScheduleEntryReqMsg to the database to retrieve the ScheduleEntry's for a person
        :param msg: although this parm is not used in the method, it is needed in the signiture when
        it is used as a replyto parm for other messages

        :return: None

        """
        self.queue_mgr.send_db_req_msg(messages.ScheduleEntryReqMsg(destination=per.DataBase,
                                                                    replyto=self.display_schedules,
                                                                    operation=messages.DBOperation.RETRIEVE_SET,
                                                                    person_id=self.person.id,
                                                                    seq_nbr=0, last_triggered=None))

    def display_schedules(self, msg: messages.ScheduleEntriesMsg):
        """
        Builds a set of widgets to display and allow the modification of a set of ScheduleEntry's

        :param msg:  a message containing a list of ScheduleEntry's
        :type msg: biometrics_tracker.ipc.messages.ScheduleEntriesMsg
        :return: None

        """
        for child in self.list_frame.children:
            if self.payload_re.match(child):
                widget = self.list_frame.nametowidget(child)
                widget.grid_remove()
        self.delete_boxes = []
        self.max_seq_nbr = 0
        row: int = 0
        for entry in msg.entries:
            if entry.seq_nbr > self.max_seq_nbr:
                self.max_seq_nbr = entry.seq_nbr
            delete_var = ttkb.IntVar()
            delete_var.set(0)
            delete_box = widgets.PayloadCheckbutton(self.list_frame, text='Del', variable=delete_var,
                                                    payload=(entry.person_id, entry.seq_nbr))
            self.delete_boxes.append((delete_var, entry.person_id, entry.seq_nbr))
            delete_box.grid(column=0, row=row, sticky=NW, padx=5, pady=5)
            label = widgets.PayloadLabel(self.list_frame, text=entry.__str__(), width=400, payload=entry)
            label.grid(column=1, row=row, sticky=NW, padx=5, pady=5)
            label.bind('<Double-1>', self.edit_entry)
            row += 1
        self.list_frame.grid()
        self.start_date_entry.focus_set()

    def edit_entry(self, event):
        """
        This method implements the logic for editing an existing ScheduleEntry

        :param event:
        :return: None

        """
        self.db_operation = messages.DBOperation.UPDATE
        for child in self.list_frame.children:
            if self.payload_re.match(child):
                widget = self.list_frame.nametowidget(child)
                if isinstance(widget, widgets.PayloadLabel):
                    widget.config(background='black')
        event.widget.config(background='blue')
        self.schedule_entry = event.widget.payload
        self.seq_nbr_entry.set_value(self.schedule_entry.seq_nbr)
        self.start_date_entry.set_date(self.schedule_entry.starts_on)
        self.end_date_entry.set_date(self.schedule_entry.ends_on)
        self.frequency_var.set(self.schedule_entry.frequency.name)
        self.dp_type_entry.set_selection(self.schedule_entry.dp_type)
        self.note_var.set(self.schedule_entry.note)
        if self.schedule_entry.suspended:
            self.suspended_var.set(1)
        else:
            self.suspended_var.set(0)
        match self.schedule_entry.frequency:
            case dp.FrequencyType.Hourly:
                self.hourly_selected()
            case dp.FrequencyType.Daily:
                self.daily_selected()
            case dp.FrequencyType.Weekly:
                self.weekly_selected()
            case dp.FrequencyType.Monthly:
                self.monthly_selected()
            case dp.FrequencyType.One_Time:
                self.once_selected()
        if self.schedule_entry.last_triggered is not None:
            self.last_triggered_var.set(self.schedule_entry.last_triggered.strftime(f'%m/%d/%Y %H:%M:%S %p'))
        self.start_date_entry.focus_set()

    def get_entry(self) -> dp.ScheduleEntry:
        """
        Retrieve a ScheduleEntry instance from the currently active SchduleEntry widget

        :return: biometric_tracker.model.datapoints.ScheduleEntry

        """
        return self.schedule_widget.get_schedule_entry()

    def validate(self):
        """
        Assures that required info has been entered before comleting a Save request.  Invokes ScheduleEntry save and
        delete handlers if the entries are valid

        :return: None

        """
        valid: bool = True
        msgs: list[str] = []
        if self.dp_type_entry.get_dp_type() is not None or len(self.frequency_var.get().strip()) > 0 or \
                len(self.note_var.get()) > 0:
            if self.dp_type_entry.get_dp_type() is None:
                msgs.append('You must select a metric type (e.g. Blood Pressure, Pulse)')
                valid = False
            if len(self.frequency_var.get().strip()) == 0:
                msgs.append('You must select a frequency (e.g. Daily, Weekly)')
                valid = False
            else:
                if self.schedule_widget.when_time_entry.get_time() is None:
                    msgs.append('You must enter a time.')
                    valid = False
                match self.frequency_var.get():
                    case dp.FrequencyType.Weekly.name:
                        if len(self.schedule_widget.weekday_entry.get_checked_days()) == 0:
                            msgs.append('You must select at least one day of the week.')
                            valid = False
                    case dp.FrequencyType.Monthly.name:
                        if len(self.schedule_widget.dom_entry.get_days_of_month()) == 0:
                            msgs.append('You must select at least one day of the month.')
                            valid = False
            if valid:
                if self.schedule_entry is None:
                    self.max_seq_nbr += 1
                self.handle_delete()
                self.submit_action()
                self.schedule_entry = None
            else:
                msg_dialog: widgets.ErrorMessageDialog = widgets.ErrorMessageDialog('Add or Edit Schedules', msgs)
                msg_dialog.show()
        else:
            self.handle_delete()

    def handle_delete(self) -> None:
        """
        Process a list of 'Delete' checkboxes, sending delete messages to the database for each checked box

        :return: None
        """
        msg_list: list[messages.ScheduleEntryReqMsg] = []
        for delete_var, person_id, seq_nbr in self.delete_boxes:
            if delete_var.get() != 0:
                msg_list.append(messages.ScheduleEntryReqMsg(destination=per.DataBase,
                                                             replyto=None,
                                                             operation=messages.DBOperation.DELETE_SINGLE,
                                                             person_id=person_id, seq_nbr=seq_nbr,
                                                             last_triggered=None))
        if len(msg_list) > 0:
            msg_list[len(msg_list)-1].replyto = self.retrieve_schedules
            for msg in msg_list:
                self.queue_mgr.send_db_req_msg(msg)


class UpcomingScheduledEventsFrame(ttkb.Frame):
    """
    This Frame prompts the user for start and end dates and displays the ScheduleEntry's that will be triggered
    during the specified period

    """
    def __init__(self, parent, person: dp.Person, queue_mgr: queues.Queues):
        ttkb.Frame.__init__(self, parent)
        self.person: dp.Person = person
        self.queue_mgr = queue_mgr
        self.delete_boxes: list[(ttkb.IntVar, str, int)] = []
        self.payload_re = re.compile('!label\w*')
        self.entry_frame = ttkb.Frame(self)
        row: int = 0
        ttkb.Label(self.entry_frame, text="View Upcoming Events").grid(column=0, row=row, columnspan=4)
        row += 2
        ttkb.Label(self.entry_frame, text=f'Person: {self.person.id} {self.person.name} DOB: {self.person.dob.month}/'
                                          f'{self.person.dob.day}/{self.person.dob.year}').grid(column=0, row=row,
                                                                                                columnspan=5, padx=5,
                                                                                                pady=5)
        row += 2
        ttkb.Label(self.entry_frame, text='Start Date:').grid(column=0, row=row, sticky=NW, padx=5, pady=5)
        self.start_date_entry: DateWidget = DateWidget(self.entry_frame, default_value=date.today())
        self.start_date_entry.grid(column=1, row=row, sticky=NW)
        ttkb.Label(self.entry_frame, text='End Date:').grid(column=2, row=row, sticky=NW, padx=5, pady=5)
        self.end_date_entry: DateWidget = DateWidget(self.entry_frame, default_value=date.today())
        self.end_date_entry.grid(column=3, row=row, sticky=NW)
        row += 2
        self.show_events_button = ttkb.Button(self.entry_frame, text='Show Events', command=self.show_events)
        self.show_events_button.grid(column=3, row=row, padx=5, pady=5)
        self.entry_frame.grid(column=0, row=0, padx=5, pady=5)
        self.list_frame = scrolled.ScrolledFrame(self, bootstyle='darkly', height=500, width=500)
        self.list_frame.grid(column=0, row=1, padx=70, pady=5)
        self.grid(padx=150)
        self.start_date_entry.set_next_entry(self.end_date_entry)
        self.end_date_entry.set_prev_entry(self.start_date_entry)
        self.end_date_entry.set_next_entry(self.show_events_button)
        self.schedule_entries: list[dp.ScheduleEntry] = []
        self.retrieve_schedules()

    def retrieve_schedules(self):
        """
         Issue a ScheduleEntryReqMsg to the database to retrieve the ScheduleEntry's for a person

         :return: None

         """
        self.queue_mgr.send_db_req_msg(messages.ScheduleEntryReqMsg(destination=per.DataBase,
                                                                    replyto=self.store_entries,
                                                                    operation=messages.DBOperation.RETRIEVE_SET,
                                                                    person_id=self.person.id,
                                                                    seq_nbr=0, last_triggered=None))

    def store_entries(self, msg: messages.ScheduleEntriesMsg):
        """
        This method is used as the 'replyto' in the ScheduleEntryReqMsg's sent by the retrieve_schedules method

        :param msg: a message containing a list of ScheduleEntry's retrieved from the database
        :type msg: biometrics_tracker.ipc.messages.ScheduleEntryMsg

        :return: None

        """
        self.schedule_entries = msg.entries
        self.start_date_entry.focus_set()

    def show_events(self):
        """
        Build a list of ScheduleEntry's that will be triggered during the specified period and insert the contents
        into a ScrollableFrame

        :return: None

        """
        for child in self.list_frame.children:
            if self.payload_re.match(child):
                widget = self.list_frame.nametowidget(child)
                widget.grid_remove()
        start_date: date = self.start_date_entry.get_date()
        end_date: date = self.end_date_entry.get_date()
        row: int = 0
        datetime_list: list[datetime] = []
        event_dict: dict[datetime: list[dp.ScheduleEntry]] = {}
        for entry in self.schedule_entries:
            event_date: date = start_date
            while event_date <= end_date and entry.starts_on <= event_date <= entry.ends_on:
                events: list[datetime] = entry.next_occurrence_today(event_date)
                if len(events) > 0:
                    for dt in events:
                        if dt in event_dict:
                            event_dict[dt].append(entry)
                        else:
                            event_dict[dt] = [entry]
                            datetime_list.append(dt)
                event_date = increment_date(event_date, days=1)
        if len(datetime_list) > 0:
            datetime_list.sort()
            for dt in datetime_list:
                for entry in event_dict[dt]:
                    text = f'{dt.strftime("%m/%d/%Y %I:%M %p")} {dp.dptype_dp_map[entry.dp_type].label()}' \
                           f' {entry.frequency.name}'
                    label = ttkb.Label(self.list_frame, text=text, width=400)
                    label.grid(column=1, row=row, sticky=NW, padx=5, pady=5)
                    row += 1
        else:
            ttkb.Label(self.list_frame, text='There are no entry sessions scheduled.').grid(column=1, row=row,
                                                                                                  sticky=tk.NW,
                                                                                                  padx=5, pady=5)
        self.list_frame.grid()
        self.start_date_entry.focus_set()

    def focus_set(self):
        """
        Delegate the focus_set method to the Starts On date widget

        :return: None
        """
        self.start_date_entry.focus_set()


class RunSchedulerFrame(ttkb.Frame):
    """
    Presents a window that allows the user to start a process that retrieves ScheduleEntry's and uses the information
    to schedule jobs using the schedule library

    """
    def __init__(self, parent, exit_action: Callable, run_action: Callable, stop_action: Callable):
        """
        Creates an instance of RunScheduler Frame

        :param parent: The GUI parent of this Frame
        :param exit_action:  a callback to be invoked when the Exit button is clicked
        :type exit_action: Callable
        :param run_action: a callback to be invoked when the Run button is clicked
        :type run_action: Callable
        :param stop_action: a callback to be invoked when the Stop button is clicked
        :type stop_action: Callable

        """
        ttkb.Frame.__init__(self, parent)
        self.run_action = run_action
        self.stop_action = stop_action
        self.row: int = 0
        ttkb.Label(self, text="Start the Scheduler and Dispatcher").grid(column=0, row=self.row, columnspan=4)
        self.row += 2
        cancel_button = ttkb.Button(self, text='Exit', command=exit_action, width=15)
        cancel_button.grid(column=0, row=self.row, sticky=NW, padx=5, pady=5)
        run_button = ttkb.Button(self, text='Start', command=self.run_action, width=15)
        run_button.grid(column=2, row=self.row, sticky=NW, padx=5, pady=5)
        if parent.dispatcher is not None:
            stop_button = ttkb.Button(self, text='Stop Dispatcher', command=self.stop_action, width=15)
            stop_button.grid(column=4, row=self.row, sticky=NW, padx=5, pady=5)
        self.row += 1
        self.grid(padx=200)
        self.id_seq_re = re.compile('(\w+)[:](\d+)')

    def show_scheduled_jobs(self, msg: messages.SchedulerCompletionMsg) -> None:
        """
        Displays a list of scheduled jobs after the SchedulerCompletionMsg is received

        :param msg: a message containing a list of schedule job
        :type msg: biometrics_tracking.ipc.messages.SchedulerCompletionMsg
        :return: None

        """
        list_frame = scrolled.ScrolledFrame(self, bootstyle='darkly', height=500, width=700)
        list_frame.grid(column=0, row=self.row, columnspan=5, padx=70, pady=5)
        self.grid(padx=150)
        if len(msg.jobs) == 0:
            ttkb.Label(list_frame, text='No Jobs where scheduled.').grid(column=0, row=self.row, sticky=NW,
                                                                         padx=5, pady=5)
        else:
            for job in msg.jobs:
                tags = job.tags.copy().pop()
                id_tag: str = tags[0]
                id_seq_tag: str = tags[1]
                dptype_tag: str = tags[2]
                id_seq = f'Person ID:Seq No.: {id_seq_tag}'
                dptype_name = f'Datapoint Type: {dptype_tag}'
                label = widgets.PayloadLabel(list_frame,
                                             text=f'{id_seq} {dptype_name} every {job.interval} '
                                                  f'{job.unit} at {job.at_time.strftime("%I:%M:%S %p)")}',
                                             payload=job, width=675)
                label.grid(column=0, row=self.row, sticky=NW, padx=5, pady=5)
                #label.bind('<Double-1>', ?)
                self.row += 1


class Application(ttkb.Window, core.CoreLogic):
    """
    This ttkbootstrap Window is the main GUI for the application.  It contains menus that invoke the various
    application functions.  The body of the Window is used to contain the various Frames that implement the
    application's functionality.
    """

    def __init__(self, config_info: config.ConfigInfo, queue_mgr: queues.Queues):
        """
        Creates an instance of gui.Application

        :param config_info: the application's configuration info
        :type config_info: biometrics_tracker.model.importers.ConfigInfo
        :param queue_mgr: sends and receives messages using the database request, database response and
        completion queues.
        :type queue_mgr: biometrics_tracker.ipc.queue_manager.Queues
        """
        ttkb.Window.__init__(self, themename="darkly", iconphoto=pathlib.Path(whereami('biometrics_tracker'), 'gui',
                                                                              'biometrics_tracker.png').__str__())
        core.CoreLogic.__init__(self, config_info, queue_mgr)
        font.nametofont('TkMenuFont').config(size=self.config_info.menu_font_size)
        font.nametofont('TkTextFont').config(size=self.config_info.text_font_size)
        font.nametofont('TkDefaultFont').config(size=self.config_info.default_font_size)
        width = 975
        height = 700
        screenwidth = self.winfo_screenwidth()
        screenheight = self.winfo_screenheight()
        alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
        self.geometry(alignstr)
        self.title(f'Biometrics Tracker version {version.__version__}')
        self.menu_bar = ttkb.Menu(self)
        self.config(menu=self.menu_bar)
        self.current_display = None
        people_menu = ttkb.Menu(self.menu_bar)
        people_menu.add_command(label='Add a New Person', command=self.add_person)
        people_menu.add_command(label="Edit a Person's Info", command=lambda: self.select_person(
            'Who do you want to edit?', self.edit_person))
        people_menu.add_command(label="Quit", command=self.close_db)
        self.menu_bar.add_cascade(label="People", menu=people_menu)
        entry_edit_menu = ttkb.Menu(self.menu_bar)
        entry_edit_menu.add_command(label='Add a New Datapoint', command=lambda: self.select_person(
            'Who are you entering data for?', self.add_datapoint))
        entry_edit_menu.add_command(label='View/Edit Biometrics History', command=lambda: self.select_person(
            'Whose history do you want to view?', self.view_edit_history))
        self.menu_bar.add_cascade(label="Entry/Edit", menu=entry_edit_menu)
        schedule_menu = ttkb.Menu(self.menu_bar)
        schedule_menu.add_command(label='Add or Edit Schedules', command=lambda: self.select_person(
            'Whose schedules do you want to work with?', self.add_edit_schedules))
        schedule_menu.add_command(label='View Upcoming Events', command=lambda: self.select_person(
            'Whose schedules do you want to work with?', self.upcoming_events))
        schedule_menu.add_command(label='Start Scheduler/Dispatcher', command=self.run_scheduler)
        self.menu_bar.add_cascade(label="Schedules", menu=schedule_menu)
        reports_menu = ttkb.Menu(self.menu_bar)
        reports_menu.add_command(label='History Report', command=lambda: self.select_person(
            'Whose history do you want to produce a report for?', self.history_report))
        self.menu_bar.add_cascade(label="Reports", menu=reports_menu)
        imp_exp_menu = ttkb.Menu(self.menu_bar)
        imp_exp_menu.add_command(label='Import from CSV', command=lambda: self.select_person(
            'Whose data are you importing?', self.import_csv))
        imp_exp_menu.add_command(label='Export to CSV/SQL', command=lambda: self.select_person(
            'Whose data are you exporting?', self.export))
        self.menu_bar.add_cascade(label="Import/Export", menu=imp_exp_menu)
        self.retrieve_installed_plugins()
        for plugin in self.plugins:
             for menu in plugin.menus:
                 menu.create_menu(not_found_action=self.plugin_not_found, selection_action=self.preproc_selection,
                                  add_menu_item=self.add_plugin_menu_item, add_menu=self.add_plugin_menu)

        config_menu = ttkb.Menu(self.menu_bar)
        config_menu.add_command(label='Install Plugin', command=self.select_install_plugin)
        config_menu.add_command(label='Uninstall Plugin', command=self.select_remove_plugin)
        self.menu_bar.add_cascade(label='Configuration', menu=config_menu)

        help_menu = ttkb.Menu(self.menu_bar)
        help_menu.add_command(label='View Help docs in browser', command=self.help_menu)
        self.menu_bar.add_cascade(label="Help", menu=help_menu)
        self.people_list: list[tuple[str, str]] = []
        self.people_listbox = None
        self.repeat_entry: bool = False
        self.scheduler: Optional[scheduler.Scheduler] = None
        self.dispatcher: Optional[dispatcher.Dispatcher] = None
        self.check_completion_queue()
        self.check_response_queue()
        self.queue_mgr.send_db_req_msg(messages.PersonReqMsg(destination=per.DataBase,
                                                             replyto=self.refresh_people_list,
                                                             person_id=None,
                                                             operation=messages.DBOperation.RETRIEVE_SET))
        self.after(600000, func=self.monitor_dispatcher_thread)

    def monitor_dispatcher_thread(self):
        if self.dispatcher is not None and not self.dispatcher.is_alive():
            self.dispatcher = None
        self.after(600000, func=self.monitor_dispatcher_thread)

    def help_menu(self):
        """
        Displays the help table of contents in the system default browser

        :return: None

        """
        if self.config_info.help_url is not None:
            webbrowser.open_new_tab(self.config_info.help_url)

    def exit_app(self, msg: messages.CompletionMsg):
        """
        End the Tkinter event loop

        :param msg: a Completion message, probably a response to a close database request
        :type msg: biometrics_tracker.ipc.messages.CompletionMsg
        :return: None

        """
        self.quit()

    def check_completion_queue(self):
        """
        Check the completion queue for messages, invoke the callback if a message is retrieved

        :return: None

        """
        core.CoreLogic.check_completion_queue(self)
        self.after(self.queue_sleep_millis, self.check_completion_queue)

    def check_response_queue(self):
        """
        Check the database response queue for messages, invoke the callback if a message is received.

        :return: None

        """
        core.CoreLogic.check_response_queue(self)
        self.after(self.queue_sleep_millis, self.check_response_queue)

    def remove_child_frame(self, frame_class):
        """
        Removes a Frame of the specified class from the Window.  This method is invoked when a application function is
         complete or canceled

        :param frame_class: the class of the Frame to be removed (e.g. gui.PersonListFrame, gui.PersonFrame,
         gui.QuickEntryFrame)
        :type frame_class: Class
        :return: None

        """
        font.nametofont('TkDefaultFont').config(family='', size=self.config_info.default_font_size)
        for widget in self.children.values():
            if isinstance(widget, frame_class):
                widget.grid_forget()
                self.grid()
                self.current_display = None

    def submit_person_update(self, person_frame: PersonFrame, operation: messages.DBOperation):
        """
        Updates the database with the result of an add or update of a person

        :param person_frame: the frame presented in the add or update process.  This method is
         invoked when the .gui.PersonFrame submit button is pressed though the submit function
         of the add_person and edit_person methods
        :type person_frame: biometrics_tracker.gui.tk_gui.PersonFrame
        :param operation: whether the Person database row is to be inserted or updated
        :type operation: biometrics_tracker.ipc.messages.DBOperation
        :return: None

        """
        person_id: str = person_frame.person_id_var.get()
        if len(person_id) == 0:
            dialogs.Messagebox.show_error(message='You must enter an ID for this person.',
                                          title='Add Person')
            person_frame.id_entry.focus_set()
            return
        name: str = person_frame.name_var.get()
        if len(name) == 0:
            dialogs.Messagebox.show_error(message='You must enter a name for this person',
                                          title='Add Person')
            person_frame.name_entry.focus_set()
            return
        dob: date = person_frame.date_entry.get_date()
        person: dp.Person = dp.Person(person_id, name, dob)
        ok = True
        for dp_widget in person_frame.dp_widgets:
            if dp_widget.tracked:
                if dp_widget.uom_widget.selected_uom:
                    tracking_cfg = dp.TrackingConfig(dp_widget.dp_type, dp_widget.uom_widget.selected_uom, True)
                    person.tracked[dp_widget.dp_type.name] = tracking_cfg
                else:
                    dp_class = dp.dptype_dp_map[dp_widget.dp_type]
                    dialogs.Messagebox.show_error(message=f'You must select a Unit of Measure for {dp_class.label()}',
                                                  title='Add Person')
                    return
        self.queue_mgr.send_db_req_msg(messages.PersonMsg(destination=per.DataBase,
                                                          replyto=self.refresh_people_list, payload=person,
                                                          operation=operation))
        self.remove_child_frame(PersonFrame)

    def add_person(self):
        """
        Presents a biometrics_tracker.gui.tk_gui.PersonFrame to allow the entry of information to a new person.  This method is invoked when the
        Person/Add a New Person menu option is selected

        :return: None
        """
        def cancel():
            self.remove_child_frame(PersonFrame)

        def submit():
            self.submit_person_update(person_frame, operation=messages.DBOperation.INSERT)

        if self.current_display:
            self.remove_child_frame(self.current_display.__class__)
        person_frame = PersonFrame(self, 'Add a New Person', cancel, submit)
        self.current_display = person_frame
        person_frame.grid(column=0, row=1)
        self.grid()
        person_frame.id_entry.focus_set()

    def edit_person(self, person_msg: messages.PersonMsg):
        """
        Presents a biometrics_tracker.gui.tk_gui.PersonFrame to allow the edit of an existing person's information.  This method in invoked
        when a person is selected after the People/Edit a Person's Information menu option is selected.

        :param person_msg: a message object containing the Person object
        :type person_msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None
        """
        def cancel():
            self.remove_child_frame(PersonFrame)

        def submit():
            self.submit_person_update(person_frame, operation=messages.DBOperation.UPDATE)

        if self.current_display:
            self.remove_child_frame(self.current_display.__class__)
        self.remove_child_frame(PeopleListFrame)
        person: dp.Person = person_msg.payload
        person_frame = PersonFrame(self, 'Edit Person', cancel, submit, person)
        self.current_display = person_frame
        person_frame.grid(column=0, row=1)
        person_frame.focus_set()

    def refresh_people_list(self, msg: messages.PeopleMsg):
        """
        Received a list of people and saves it for future use

        :param msg: a message containing a list of Person instances
        :type msg: biometrics_tracker.ipc.messages.PeopleMsg

        :return: None

        """
        self.people_list = msg.payload

    def select_person(self, title: str, callback: Callable):
        """
        Presents a biometrics_tracker.gui.tk_gui.PeopleListFrame to allow the selection of a person for further processing.

        :param title: a title to be displayed at the top of the Frame
        :type title: str
        :param callback: a callback to be invoked when a person is selected
        :type callback: Callable
        :return: None
        """
        if self.current_display:
            self.remove_child_frame(self.current_display.__class__)
        self.people_listbox = PeopleListFrame(self, title, self.people_list, self.queue_mgr, callback)
        self.current_display = self.people_listbox
        self.people_listbox.grid(column=0, row=1, sticky=NSEW, padx=300, pady=5)
        self.grid()
        self.people_listbox.focus_set()

    def add_datapoint(self, person_msg: messages.PersonMsg):
        """
        Presents a biometrics_tracker.gui.tk_gui.QuickEntryFrame to allow a set of metrics for a date and time to be entered.  This method
        is invoked when a person is selected after the Biometrics/Quick Entry menu option is selected.

        :param person_msg: the  DB response message contain the Person instance
        :type person_msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None
        """
        def cancel():
            self.repeat_entry = False
            self.remove_child_frame(NewDataPointFrame)

        def submit(repeat: bool = False):
            self.repeat_entry = repeat
            dp_list: list[dp.DataPoint] = []
            taken_date: date = frame.date_widget.get_date()
            taken_time: time = frame.time_widget.get_time()
            taken: datetime = datetime(year=taken_date.year, month=taken_date.month, day=taken_date.day,
                                       hour=taken_time.hour, minute=taken_time.minute)
            for bio_entry in frame.bio_entries:
                self.save_datapoint(frame.person, taken, bio_entry, delete_if_zero=False,
                                    db_operation=messages.DBOperation.INSERT)
            if self.current_display:
                if self.repeat_entry:
                    frame.clear_entries()
                    frame.date_widget.focus_set()
                else:
                    self.remove_child_frame(NewDataPointFrame)

        person_id = self.people_listbox.selected_person()
        self.remove_child_frame(PeopleListFrame)
        if person_id:
            person: dp.Person = person_msg.payload
            frame = NewDataPointFrame(self, person, cancel, submit)
            self.current_display = frame
            frame.date_widget.focus_set()

    def view_edit_history(self, person_msg: messages.PersonMsg):
        """
        Presents a biometrics_tracker.gui.tk_gui.ViewEditBiometricsFrame that allows the retrieval and editing of datapoint
        for the selected person that are within a specified date range.

        :param person_msg: a message object containing the a Person object
        :type person_msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None
        """
        def cancel():
            self.remove_child_frame(ViewEditBiometricsFrame)

        def submit():
            for row, dp_info in self.current_display.delete_boxes.items():
                taken, delete_var = dp_info
                if delete_var.get() > 0:
                    self.queue_mgr.send_db_req_msg(messages.DataPointReqMsg(destination=per.DataBase, replyto=None,
                                                                            person_id=person.id, start=taken,
                                                                            end=taken,
                                                                            operation=messages.DBOperation.DELETE_SET))
            if self.current_display.is_changed():
                datapoint: dp.DataPoint = self.current_display.get_datapoint()
                self.save_datapoint(person, datapoint.taken, widget=self.current_display.get_metric_widget(),
                                    delete_if_zero=True, db_operation=messages.DBOperation.UPDATE)
            self.remove_child_frame(ViewEditBiometricsFrame)

        self.remove_child_frame(PeopleListFrame)
        person: dp.Person = person_msg.payload
        frame = ViewEditBiometricsFrame(self, person, self.queue_mgr, cancel, submit)
        self.current_display = frame
        frame.start_date_widget.focus_set()

    def history_report(self, person_msg: messages.PersonMsg):
        """
        Presents a gui.BiometricsReportFrame that allows the accepts a date range and produces a report for the selected
        person.

        :param person_msg: a message object containing a Person instance
        :type person_msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None
        """
        def close():
            if self.current_display.rptf is not None:
                self.current_display.rptf.close()
            self.remove_child_frame(BiometricsReportPromptFrame)

        self.remove_child_frame(PeopleListFrame)
        person: dp.Person = person_msg.payload
        # TODO: Figure out how to apply the monospaced font directly to the ScrolledText widget
        font.nametofont('TkDefaultFont').config(family='courier', size=self.config_info.default_font_size)
        frame = BiometricsReportPromptFrame(self, person, self.queue_mgr, close)
        self.current_display = frame
        frame.start_date_widget.focus_set()

    def import_csv(self, person_msg: messages.PersonMsg):
        """
        Presents a gui.CSVImportFrame to allow a CSV file to be imported.  This method is invoked when a person is
        selected after the Biometrics/Import CSV File menu option is selected.

        :param person_msg: a message object containing a Person instance
        :type person_msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None
        """
        def cancel():
            self.remove_child_frame(CSVImportFrame)

        def proceed():
            if frame.import_file_name():
                self.remove_child_frame(CSVImportFrame)
                person: dp.Person = person_msg.payload
                csv_fields: dict[int, str] = frame.indexed_fields()
                importer = imp.CSVImporter(self.queue_mgr, person, frame.import_file_name(),
                                           frame.header_rows(), frame.use_prev_row_date, csv_fields,
                                           completion_dest=self.import_process_complete)
                importer.start()

        person = person_msg.payload
        self.remove_child_frame(PeopleListFrame)
        frame = CSVImportFrame(self, person, cancel, proceed, self.config_info)
        self.current_display = frame
        frame.widgets[0].focus_set()

    def import_process_complete(self, msg: messages.ImportExportCompletionMsg):
        """
        This method is specified as the completion callback for the import process

        :param msg: the completion message
        :type msg: biometrics_tracker.ipc.messages.CompletionMsg
        :return: None

        """
        status_popup = ImportExportStatusDialog(self, msg, ['Import Process Complete', 'Import Process'])
        status_popup.show()

    def export(self, person_msg: messages.PersonMsg):
        """
        Presents a gui.CSVImportFrame to allow a CSV file to be imported.  This method is invoked when a person is
        selected after the Biometrics/Import CSV File menu option is selected.

        :param person_msg: a message object containing a Person instance
        :type person_msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None
        """
        def cancel():
            self.remove_child_frame(ExportFrame)

        def proceed():
            if frame.get_export_dir_path() is not None:
                self.remove_child_frame(ExportFrame)
                person: dp.Person = person_msg.payload
                match frame.get_export_type():
                    case exp.ExportType.CSV:
                        exporter = exp.CSVExporter(queue_mgr=self.queue_mgr, person=person,
                                                   start_date=frame.get_start_date(), end_date=frame.get_end_date(),
                                                   date_fmt= frame.get_date_format(), time_fmt=frame.get_time_format(),
                                                   indexed_fields=frame.indexed_fields(),
                                                   uom_handling=frame.get_uom_handling_type(),
                                                   include_header_row=frame.get_include_header(),
                                                   export_dir_path=frame.get_export_dir_path(),
                                                   completion_destination=self.export_process_complete)
                    case exp.ExportType.SQL:
                        exporter = exp.SQLiteExporter(queue_mgr=self.queue_mgr, person=person,
                                                      start_date=frame.get_start_date(), end_date=frame.get_end_date(),
                                                      date_fmt=frame.get_date_format(),
                                                      time_fmt=frame.get_time_format(),
                                                      indexed_fields=frame.indexed_fields(),
                                                      uom_handling=frame.get_uom_handling_type(),
                                                      export_dir_path=frame.get_export_dir_path(),
                                                      completion_destination=self.export_process_complete)

                exporter.start()

        person = person_msg.payload
        self.remove_child_frame(PeopleListFrame)
        frame = ExportFrame(self, person, cancel, proceed, self.config_info)
        self.current_display = frame
        frame.focus_set()

    def export_process_complete(self, msg: messages.ImportExportCompletionMsg):
        """
        This method is specified as the completion callback for the export process

        :param msg: the completion message
        :type msg: biometrics_tracker.ipc.messages.CompletionMsg
        :return: None

        """
        status_popup = ImportExportStatusDialog(self, msg, ['Export Process Complete', 'Export Process'])
        status_popup.show()
        ...

    def add_edit_schedules(self, person_msg: messages.PersonMsg):
        """
        Handles the Add/Edit Schedule Entries function

        :param person_msg: a message containing a Person instance for the person whose schedules will be maintained.
        :type person_msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None

        """
        def cancel():
            self.remove_child_frame(AddEditScheduleFrame)

        def submit():
            schedule_entry: dp.ScheduleEntry = frame.get_entry()
            schedule_entry.starts_on = frame.start_date_entry.get_date()
            schedule_entry.ends_on = frame.end_date_entry.get_date()
            schedule_entry.dp_type = frame.dp_type_entry.get_dp_type()
            schedule_entry.frequency = dp.frequency_name_map[frame.frequency_var.get()]
            self.queue_mgr.send_db_req_msg(messages.ScheduleEntryMsg(destination=per.DataBase,
                                                                     replyto=lambda msg=person_msg: self.add_edit_schedules(msg),
                                                                     operation=frame.db_operation,
                                                                     person_id=person.id,
                                                                     seq_nbr=schedule_entry.seq_nbr,
                                                                     payload=schedule_entry))
            self.queue_mgr.send_db_req_msg(messages.ScheduleEntryReqMsg(destination=per.DataBase,
                                                                        replyto=frame.display_schedules,
                                                                        operation=messages.DBOperation.RETRIEVE_SET,
                                                                        person_id=person.id,
                                                                        seq_nbr=0, last_triggered=None))
            frame.clear_entry()

        person = person_msg.payload
        if isinstance(self.current_display, PeopleListFrame):
            self.remove_child_frame(PeopleListFrame)
        frame = AddEditScheduleFrame(self, person=person, queue_mgr=self.queue_mgr, cancel_action=cancel,
                                     submit_action=submit)
        self.current_display = frame
        frame.focus_set()

    def upcoming_events(self, person_msg: messages.PersonMsg):
        """
        Build and display a list of ScheduleEntry's that will be triggered during the specified series

        :param person_msg: a message containing a Person instance for the person whose ScheduleEntry's will be dislayed
        :type person_msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None

        """
        person = person_msg.payload
        if isinstance(self.current_display, PeopleListFrame):
            self.remove_child_frame(PeopleListFrame)
        frame = UpcomingScheduledEventsFrame(self, person=person, queue_mgr=self.queue_mgr)
        self.current_display = frame
        frame.focus_set()

    def run_scheduler(self) -> None:
        """
        Presents the RunSchedulerFrame and starts biometrics_tracker.main.scheduler.Scheduler in a separate thread

        :return: None

        """
        def exit_run_scheduler() -> None:
            self.remove_child_frame(RunSchedulerFrame)

        def run() -> None:
            self.scheduler = scheduler.Scheduler(self.config_info, self.queue_mgr,
                                                 start_dispatcher=(self.dispatcher is None),
                                                 completion_replyto=frame.show_scheduled_jobs)
            self.scheduler.start()
            if self.dispatcher is None:
                self.dispatcher = self.scheduler.dispatcher

        def stop() -> None:
            if self.dispatcher is not None:
                self.dispatcher.stop()
                self.dispatcher = None
            self.scheduler.stop()
            self.remove_child_frame(RunSchedulerFrame)

        if self.current_display is not None:
            self.remove_child_frame(self.current_display.__class__)

        frame = RunSchedulerFrame(self, exit_action=exit_run_scheduler, run_action=run, stop_action=stop)
        self.current_display = frame
        frame.focus_set()

    def select_install_plugin(self) -> None:
        """
        Select a Plugin Specification File to be installed in the app's plugin folder.  The selected file will be
        copied to that folder.  The application must be restarted for the plugin's menu to appear in the
        menu bar

        :return: None

        """
        file_str: Optional[str] = filedialog.askopenfilename(filetypes=[('JSON', '.json')],
                                                             title='Select a Plugin Specification File')
        if len(file_str.strip()) > 0:
            file_path: pathlib.Path = pathlib.Path(file_str)
            self.install_plugin(file_path)

    def select_remove_plugin(self) -> None:
        """
        Select a Plugin Specification File from the app's plugin folder to be deleted.

        :return: None

        """
        file_str: Optional[str] = filedialog.askopenfilename(initialdir=self.config_info.plugin_dir_path.__str__(),
                                                             filetypes=[('JSON', '.json')],
                                                             title='Select a Plugin Specification File to Remove')
        if len(file_str.strip()) > 0:
            file_path: pathlib.Path = pathlib.Path(file_str)
            file_path.unlink(missing_ok=True)

    def add_plugin_menu_item(self, menu: ttkb.Menu, label: str, action: Callable) -> None:
        """
        This method is provided as a callback to the biometrics_tracker.plugin.plugin.PluginMenu.create_menu method

        :param menu: the parent Menu object
        :type menu: ttkbootstrap.Menu
        :param label: the text to be used for the menu item label
        :param label: str
        :param action: the callback to be invoked when the menu item is selected
        :param action: Callable
        :return: None

        """
        menu.add_command(label=label, command=action)

    def add_plugin_menu(self, label: str) -> Any:
        """
        This method is provided to the biometrics_tracker.plugin.plugin.PluginMenu.create_menu method.

        :param self:
        :param label:
        :return:
        """
        menu = ttkb.Menu(master=self.menu_bar)
        self.menu_bar.add_cascade(label=label, menu=menu)
        return menu

    def plugin_not_found(self) -> None:
        """
        This is a dummy placeholder to be used an action method in cases when a plugin module can not be imported
        or an entry point is not found within the module.

        :return: None

        """
        pass

    def preproc_selection(self, sel_person: bool, sel_date_rng: bool, sel_dp_type: bool, action: Callable) -> None:
        """
        This method is provided as an action call back to the biometrics_tracker.plugin.plugin.PluginMenu.create_menu
        method.  It creates and displays an instance of DataPointSelectionFrame to collect selection criteria.

        :param sel_person: will Person ID be part of the selection criteria
        :type sel_person: bool
        :param sel_date_rng: will date range be part of the selection criteria
        :type sel_date_rng: bool
        :param sel_dp_type: will DataPointType be part of the selection criteria
        :type sel_dp_type: bool
        :param action: the plugin's entry point
        :type action: Callable
        :return: None

        """
        def cancel():
            self.remove_child_frame(self.current_display.__class__)

        if self.current_display is not None:
            self.remove_child_frame(self.current_display.__class__)
        people_list: list[tuple[str, str]] = []
        if sel_person:
            people_list = self.people_list
        self.current_display = DataPointSelectionFrame(parent=self, people_list=people_list,
                                                       sel_date_rng=sel_date_rng, sel_dp_type=sel_dp_type,
                                                       cancel_action=cancel, proceed_action=action)
        self.current_display.focus_set()


class ScheduledEntryWindow(ttkb.Window, core.CoreLogic):
    """
    This ttkbootstrap Window handles the scheduled entry of a single metric.  The person is the person is the
    owner of the schedule entry and date is assumed to be the current date.
    """

    def __init__(self, config_info: config.ConfigInfo, queue_mgr: queues.Queues, schedule: dp.ScheduleEntry,
                 person: dp.Person):
        """
        Creates an instance of biometrics-tracker.gui.tkgui.ScheduledEntry

        :param config_info: the application's configuration info
        :type config_info: biometrics_tracker.model.importers.ConfigInfo
        :param queue_mgr: managers send and receiving messages using the database request, database response and
        completion queues.
        :type queue_mgr: biometrics_tracker.ipc.queue_manager.Queues
        :param schedule: the ScheduleEntry that triggered the job
        :type schedule: biometrics_tracker.model.datapoints.ScheduleEntry
        :param person: the Person the Schedules are related to
        :type person: biometrics_tracker.model.datapoints.Person

        """
        ttkb.Window.__init__(self, themename="darkly",
                             iconphoto=pathlib.Path(whereami('biometrics_tracker'), 'gui',
                                                    'biometrics_tracker.png').__str__())
        core.CoreLogic.__init__(self, config_info, queue_mgr)
        self.person: dp.Person = person
        self.schedule: dp.ScheduleEntry = schedule
        font.nametofont('TkMenuFont').config(size=self.config_info.menu_font_size)
        font.nametofont('TkTextFont').config(size=self.config_info.text_font_size)
        font.nametofont('TkDefaultFont').config(size=self.config_info.default_font_size)
        width = 975
        height = 700
        screenwidth = self.winfo_screenwidth()
        screenheight = self.winfo_screenheight()
        alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
        self.geometry(alignstr)
        self.title(f'Biometrics Tracker version {version.__version__}')
        self.menu_bar = ttkb.Menu(self)
        self.config(menu=self.menu_bar)
        self.current_display = None
        help_menu = ttkb.Menu(self.menu_bar)
        help_menu.add_command(label='View Help docs in browser', command=self.help_menu)
        help_menu.add_command(label='Quit', command=self.close_db)
        self.menu_bar.add_cascade(label="Help", menu=help_menu)
        self.row: int = 0
        ttkb.Label(self, text="Add New Datapoint").grid(column=0, row=self.row, columnspan=4)
        self.row += 1
        ttkb.Label(self, text=f'Person: {self.person.id} {self.person.name} DOB: {self.person.dob.month}/'
                   f'{self.person.dob.day}/{self.person.dob.year}').grid(column=0, row=self.row,
                                                                         columnspan=5, padx=5, pady=5)
        self.row += 1
        ttkb.Label(self, text=f'Person: {self.person.id} {self.person.name} DOB: {self.person.dob.month}/'
                   f'{self.person.dob.day}/{self.person.dob.year}').grid(column=0, row=1,
                                                                         columnspan=5, padx=5, pady=5)

        ttkb.Label(self, text=f'Schedule Entry: {self.schedule.__str__()}').grid(column=0, row=self.row, columnspan=4)
        self.row += 1
        ttkb.Label(self, text=self.schedule.note, width=50).grid(column=0, row=self.row, columnspan=4)
        self.row += 2
        ttkb.Label(self, text="Date:").grid(column=0, row=self.row, padx=5, pady=5)
        self.date_widget = DateWidget(self, default_value=date.today())
        self.date_widget.grid(column=1, row=self.row, padx=5, pady=5)
        ttkb.Label(self, text="Time:").grid(column=2, row=self.row, padx=5, pady=5)
        self.time_widget = TimeWidget(self, default_value=datetime.now().time())
        self.time_widget.grid(column=3, row=self.row, padx=5, pady=5)
        track_cfg = self.person.dp_type_track_cfg(self.schedule.dp_type)
        self.row += 1
        if track_cfg is not None:
            self.widget = MetricWidgetFactory.make_widget(self, track_cfg, None, show_note_field=True)
            self.widget.grid(column=0, row=self.row, columnspan=3, sticky=NW)
            self.time_widget.set_next_entry(self.widget)
        else:
            raise ValueError(f'{self.schedule.dp_type.name} is not tracked for {self.person.name}')

        self.row += 2
        ttkb.Button(self, text="Cancel", command=self.close_db).grid(column=0, row=self.row, sticky=NW, padx=10)
        ttkb.Button(self, text="Suspend Schedule", command=self.suspend_schedule).grid(column=1, row=self.row,
                                                                                       sticky=NW,  padx=10)
        ttkb.Button(self, text="Save", command=self.submit).grid(column=3, row=self.row, sticky=NW, padx=10)
        self.date_widget.set_next_entry(self.time_widget)
        self.time_widget.set_prev_entry(self.date_widget)
        self.grid()
        self.widget = None
        self.row: int = 0

    def run(self) -> None:
        """
        This method is invoked when the ScheduledEntry thread is started.  It sends a request to the persistance
        manager to retrieve the Person instance for the person associated with the scheduled job that invoked
        this class

        :return: None

        """
        self.check_completion_queue()
        self.check_response_queue()
        self.mainloop()

    def make_metric_widget(self, msg: messages.PersonMsg):
        """
        A callback specified as the replyto Callable in the message sent by the run method

        :param msg: a message containing a Person instance
        :type msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None

        """
        track_cfg = self.person.dp_type_track_cfg(self.schedule.dp_type)
        if track_cfg is not None:
            self.widget = MetricWidgetFactory.make_widget(self, track_cfg, None, show_note_field=True)
            self.widget.grid(column=0, row=4, columnspan=3, sticky=NW)
            self.time_widget.set_next_entry(self.widget)
        else:
            raise ValueError(f'{self.schedule.dp_type.name} is not tracked for {self.person.name}')

    def close_db(self) -> None:
        """
        Send a CloseDataBaseReqMsg to the database manager, with exit_app method as the reply_to Callable

        :return: None
        """
        self.queue_mgr.send_db_req_msg(messages.CloseDataBaseReqMsg(destination=per.DataBase, replyto=self.exit_app))

    def exit_app(self, msg: messages.CompletionMsg) -> None:
        """
        Ends the GUI event loop

        :param msg: a Completion message, usually a response to a Close Database request
        :return: None

        """
        self.quit()

    def help_menu(self):
        """
        Displays the help table of contents in the system default browser

        :return: None

        """
        if self.config_info.help_url is not None:
            webbrowser.open_new_tab(self.config_info.help_url)

    def check_completion_queue(self):
        """
        Check the completion queue for messages, invoke the callback if a message is retrieved

        :return: None

        """
        core.CoreLogic.check_completion_queue(self)
        self.after(self.queue_sleep_millis, self.check_completion_queue)

    def check_response_queue(self):
        """
        Check the database response queue for messages, invoke the callback if a message is received.

        :return: None

        """
        core.CoreLogic.check_response_queue(self)
        self.after(self.queue_sleep_millis, self.check_response_queue)

    def remove_child_frame(self, frame_class):
        """
        Removes a Frame of the specified class from the Window.  This method is invoked when a application function is
         complete or canceled

        :param frame_class: the class of the Frame to be removed (e.g. gui.PersonListFrame, gui.PersonFrame,
         gui.QuickEntryFrame)
        :type frame_class: Class
        :return: None

        """
        font.nametofont('TkDefaultFont').config(family='', size=self.config_info.default_font_size)
        for widget in self.children.values():
            if isinstance(widget, frame_class):
                widget.grid_forget()
                self.grid()
                self.current_display = None

    def get_date(self) -> date:
        """
        Returns the date the metric reading was taken

        :return: the date the metric reading was taken
        :rtype: datetime.date

        """
        return self.date_widget.get_date()

    def get_time(self) -> time:
        """
        Returns the time the metric reading was taken

        :return: the time the metric reading was taken
        :rtype: datetime.time

        """
        return self.time_widget.get_time()

    def submit(self):
        """
        A callback invoked when the Submit button is clicked

        :return: None

        """
        taken: datetime = mk_datetime(self.get_date(), self.get_time())
        self.save_datapoint(self.person, taken, self.widget, delete_if_zero=False,
                            db_operation=messages.DBOperation.INSERT)
        self.close_db()

    def suspend_schedule(self):
        """
        A callback invoked when the Suspend this Schedule button is clicked

        :return: None

        """
        self.schedule.suspended = True
        self.queue_mgr.send_db_req_msg(messages.ScheduleEntryMsg(destination='per.DataBase', replyto=None,
                                                                 person_id=self.person.id,
                                                                 seq_nbr=self.schedule.seq_nbr,
                                                                 payload=self.schedule,
                                                                 operation=messages.DBOperation.UPDATE))
        self.close_db()


class DataPointSelectionFrame(ttkb.Frame, core.DataPointSelection):
    """
    A GUI frame that collects selection criteria for retrieving DataPoints from the database

    """
    def __init__(self, parent: Application, people_list: list[tuple[str, str]], sel_date_rng: bool, sel_dp_type: bool,
                 cancel_action: Callable, proceed_action: Callable):
        """
        Creates an instance of DataPointSelectionFrame.  This class inherits from ttkbootstrap.Frame and
        biometrics_tracker.main.core.DataPointSelection

        :param parent: the Application instance that is the GUI parent of this frame
        :type parent: biometrics_tracker.gui.tk_gui.Application
        :param people_list: a list of tuples each containing a Person ID and Name
        :type people_list: list[tuple[str, str]]
        :param sel_date_rng: should a date range selection be presented
        :type sel_date_rng: bool
        :param sel_dp_type: show a DataPointType selection be presented
        :type sel_dp_type: bool
        :param cancel_action: a Callable to be invoked when the user clicks the Cancel button
        :type cancel_action: Callable
        :param proceed_action: a Callable to be invoked when the user clicks the Process button
        :type proceed_action: Callable

        """
        ttkb.Frame.__init__(self, master=parent)
        core.DataPointSelection.__init__(self, parent=parent, people_list=people_list, sel_date_rng=sel_date_rng,
                                         sel_dp_type=sel_dp_type, action=proceed_action,
                                         get_date_range=self.get_date_range,
                                         get_dp_type_selection=self.get_dp_type_selection)
        self.parent: Application = parent
        self.people_list_frame: Optional[PeopleListFrame] = None
        self.start_date_widget: Optional[widgets.DateWidget] = None
        self.end_date_widget: Optional[widgets.DateWidget] = None
        self.dp_widget: Optional[widgets.DataPointTypeComboWidget] = None
        self.focus_set_gui = None
        row = 0
        if len(people_list) > 0:
            self.people_list_frame = PeopleListFrame(parent=self, title='Select a person',
                                                     people_list=self.people_list, queue_mgr=parent.queue_mgr,
                                                     callback=self.get_person)
            self.people_list_frame.grid(column=0, row=row, columnspan=4, padx=5, pady=5, sticky=tk.NSEW)
            self.focus_set_gui = self.people_list_frame
            row += 1
            self.person_label = ttkb.Label(master=self, text='Double click to select a person')
            self.person_label.grid(column=0, row=row, columnspan=4, padx=5, pady=5, sticky=tk.EW)
            row += 1
        if sel_date_rng:
            ttkb.Label(master=self, text='Start Date: ', anchor=tk.E).grid(column=0, row=row, padx=5, pady=5)
            self.start_date_widget = widgets.DateWidget(parent=self)
            self.start_date_widget.grid(column=1, row=row, padx=5, pady=5, sticky=tk.W)
            ttkb.Label(master=self, text='End Date: ', anchor=tk.E).grid(column=2, row=row, padx=5, pady=5)
            self.end_date_widget = widgets.DateWidget(parent=self)
            self.end_date_widget.grid(column=3, row=row, padx=5, pady=5, sticky=tk.W)
            if self.focus_set_gui is None:
                self.focus_set_gui = self.start_date_widget
            if len(people_list) > 0:
                self.start_date_widget.set_prev_entry(self.people_list_frame)
            self.start_date_widget.set_next_entry(self.end_date_widget)
            self.end_date_widget.set_prev_entry(self.start_date_widget)
            row += 1
        if sel_dp_type:
            ttkb.Label(master=self, text='Datapoint Type:', anchor=tk.E).grid(column=0, row=row, padx=5, pady=5)
            self.dp_widget = widgets.DataPointTypeComboWidget(parent=self, person=None)
            self.dp_widget.grid(column=1, row=row, columnspan=2, padx=5, pady=5, sticky=tk.NSEW)
            if sel_date_rng:
                self.end_date_widget.set_next_entry(self.dp_widget)
            row += 1
            if self.focus_set_gui is None:
                self.focus_set_gui = self.dp_widget
        elif sel_date_rng:
            if len(people_list) > 0:
                self.end_date_widget.set_next_entry(self.people_list_frame)
            else:
                self.end_date_widget.set_next_entry(self.start_date_widget)

        cancel_button = ttkb.Button(master=self, text='Cancel', command=cancel_action)
        cancel_button.grid(column=0, row=row, padx=5, pady=5, sticky=tk.EW)
        proceed_button = ttkb.Button(master=self, text='Proceed', command=self.proceed)
        proceed_button.grid(column=3, row=row, padx=5, pady=5, sticky=tk.EW)

        self.grid(padx=50, pady=25)
        self.focus_set()

    def focus_set(self):
        """
        Delegate focus_set invocations to the first prompt on the display

        :return: None

        """
        self.focus_set_gui.focus_set()

    def get_person(self, msg: messages.PersonMsg) -> None:
        """
        If person is part of the selection criteria, set the selected Person ID property.  This method is the
        callback routine for the PeopleListFrame

        :param msg: the Person instance retrieved from the database
        :type msg: biometrics_tracker.ipc.messages.PersonMsg
        :return: None

        """
        self.person = None
        if len(self.people_list) > 0:
            self.person = msg.payload
            self.person_label['text'] = self.person.__str__()

    def get_date_range(self) -> None:
        """
        If date range is part of the selection criteria, set the start date and end date properties from the
        Start Date and End Date widgets, otherwise set these properties to None

        :return: None

        """
        self.start_date = None
        self.end_date = None
        if self.sel_date_rng:
            self.start_date = self.start_date_widget.get_date()
            self.end_date = self.end_date_widget.get_date()

    def get_dp_type_selection(self) -> None:
        """
        If DataPointType is part of the selection criteria, set the DataPointType property from the
        DataPointTypeComboWidget, otherwise set the property to None

        :return: None

        """
        self.dp_type = None
        if self.sel_dp_type:
            self.dp_type = self.dp_widget.get_dp_type()


if __name__ == '__main__':
    def cancel():
        pass

    def proceed():
        pass

    window = ttkb.Window(title='Selection Test', themename='darkly')
    # test code here

    ttkb.Button(master=window, text='Quit', command=window.quit).grid(column=0, row=1)
    window.mainloop()

