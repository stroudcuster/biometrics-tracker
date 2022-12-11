from abc import abstractmethod
from datetime import datetime, date, time
from decimal import Decimal
import pathlib
import re
import tkinter as tk
import tkinter.simpledialog as tksimpedialog
from tkinter import ttk
import ttkbootstrap as ttkb
import ttkbootstrap.dialogs.dialogs as dialogs
import ttkbootstrap.scrolled as scrolled
from ttkbootstrap.validation import add_validation
from ttkbootstrap.constants import *
from typing import Any, Callable, Literal, Optional, Union

from biometrics_tracker.gui.validators import month_validator, day_validator, year_validator, hour_validator, \
    minute_validator
import biometrics_tracker.model.exporters as exp
import biometrics_tracker.model.importers as imp
from biometrics_tracker.model import uoms as uoms, datapoints as dp
import biometrics_tracker.utilities.utilities as utilities

METRIC_WIDGET_MIN_LBL_WIDTH: int = 20


class Radiobutton(ttkb.Radiobutton):
    """
    Subclass of ttkbootstrap.Radiobutton that changes it appears when it is in focus, so that the user
    can tell when that can use the space bar to select or unselect a button

    """
    def __init__(self, parent, text: str, value: str, variable: ttkb.StringVar, command=None,
                 width=None):
        ttkb.Radiobutton.__init__(self, parent, text=text, value=value, variable=variable,
                                  command=command, width=width, bootstyle='primary')
        self.bind('<FocusIn>', self.focus_in)
        self.bind('<FocusOut>', self.focus_out)

    def focus_in(self, event) -> None:
        """
        Handler for the <FocusIn> event. Changes config parm bootstyle to 'toolbutton'
        :param event:
        :return: None
        """
        self.configure(bootstyle='toolbutton')

    def focus_out(self, event):
        """
        Handler for the <FocusOut> event.  Changes config parm bootstyle to 'primary'
        :param event:
        :return:
        """
        self.configure(bootstyle='primary')


class Checkbutton(ttkb.Checkbutton):
    """
    Subclass of ttkbootstrap.Checkbutton that changes it appears when it is in focus, so that the user
    can tell when that can use the space bar to select or unselect a checkbox
    """
    def __init__(self, parent, text, variable, command=None, padding=None, width=None):
        ttkb.Checkbutton.__init__(self, parent, text=text, variable=variable, command=command, padding=padding,
                                  width=width, bootstyle='primary')
        self.bind('<FocusIn>', self.focus_in)
        self.bind('<FocusOut>', self.focus_out)
        self.row: int = -1
        self.column: int = -1

    def focus_in(self, event):
        """
        Handler for the <FocusIn> event. Changes config parm bootstyle to 'toolbutton'
        :param event:
        :return: None
        """
        self.configure(bootstyle='toolbutton')

    def focus_out(self, event):
        """
        Handler for the <FocusOut> event.  Changes config parm bootstyle to 'primary'
        :param event:
        :return:
        """
        self.configure(bootstyle='primary')


class EntryWidget:
    """
    An abstract base class for entry label/widget pairs
    """
    def __init__(self, parent, label_text: Optional[str], entry_width:int, regex_str: Optional[str] = None):
        """
        Create an instance of EntryWidget
        
        :param parent: the GUI parent of this class
        :param label_text: the text to be used in creating the label
        :type label_text: str`
        :param regex_str:
        """
        self.parent = parent
        self.label_text = label_text
        if regex_str is not None:
            self.regex_pattern: Optional[re.Pattern] = re.compile(regex_str)
        else:
            self.regex_pattern = None
        self.strvar = ttkb.StringVar()
        self.entry = ttkb.Entry(master=parent, textvariable=self.strvar, validate='focusout', width=entry_width,
                                validatecommand=self.validate, invalidcommand=self.invalid)

    def invalid(self):
        """
        This method is used as an 'invalidcommand' callback.  It is invoked when validation fails.  It sets the widget
        background color to red, which gives it a red border, then calls focus_set on the widget

        :return: None

        """
        self.entry.config(background='red')
        self.focus_set()

    def focus_set(self):
        """
        Delegates focus_set calls to the entry widget

        :return: None

        """
        self.entry.focus_set()
        self.entry.selection_range('0', tk.END)

    def bind(self, sequence: str | None = ...,
             func: Callable[[tk.Event], Any] | None = ...,
             add: Literal["", "+"] | bool | None = ..., ) -> str:
        """
        Delegate bind calls to the entry widget

        :param sequence: the name of the event to be bound
        :type sequence: str | None
        :param func: the callback to be invoked when the event is captured
        :type func: Callable[[tk.Event], Any]
        :param add: allows multiple bindings for single event
        :type add: Literal["", "+"] | bool | None
        :return: None

        """
        return self.entry.bind(sequence, func, add)

    def grid(self, **kwargs):
        """
        Delegate grid calls to the entry widget

        :param kwargs: key word args
        :type kwargs: dict[str,Any]
        :return: None

        """
        self.entry.grid(kwargs)

    def set_regex(self, regex_str: str) -> None:
        """
        Set the regular expression used to validate user input

        :param regex_str: a regular expression
        :type regex_str: str
        :return: None

        """
        self.regex_pattern = re.compile(regex_str)

    def apply_regex(self, value=None) -> Optional[tuple[Any, ...]]:
        """
        Apply the widget's regex either to the proved string or to the value entered to the widget prompt

        :param value: if not None, the regex will be applied to this value
        :type value: str
        :return: returns the groups collected by the regex
        :rtype: Optional[tuple[Any, ...]]

        """
        if value is None:
            match = self.regex_pattern.match(self.strvar.get())
        else:
            match = self.regex_pattern.match(value)

        if match is None:
            return None
        else:
            return match.groups()

    def get_var(self) -> ttkb.StringVar:
        """
        Returns the StringVar associated with the entry widget

        :return: the StringVar associated with the entry widget
        :rtype: ttkb.StringVar

        """
        return self.strvar

    @abstractmethod
    def get_value(self):
        """
        An abstract method to be implemented by subclasses to return the entry widget value

        :return: the entry widget value
        :rtype: implementation dependant

        """
        ...

    @abstractmethod
    def set_value(self, value: Union[int, Decimal, str]) -> None:
        """
        An abstract method implemented by subclasses to set the entry widget value

        :param value:  value to be set
        :type value: Union[int, Decimal, str]
        :return: None

        """
        ...

    @abstractmethod
    def validate(self) -> int:
        """
        An abstract method implemented by subclasses to set the entry widget value

        :return: 1 if valid, 0 if not

        """
        ...


class TextWidget(EntryWidget):
    """
    A regex validated text entry widget

    """
    def __init__(self, parent, label_text: str, entry_width: int, regex_str: Optional[str] = None):
        """
        Creates and instance of TextWidget

        :param parent: the GUI parent of this widget
        :param label_text: the text for the label that will be associated with the widget.  The label is not
            created with the entry widget, but the label text is used to create ValueError exception messages
        :type label_text: str
        :param regex_str: The regular expression that will be used to validate data entered to the widget
        :type regex_str: str

        """
        EntryWidget.__init__(self, parent=parent, label_text=label_text, entry_width=entry_width, regex_str=regex_str)

    def validate(self) -> int:
        """
        A validation callback for the validationcommand parameter of the tkinter Entry widget

        :return: 1 if validate, otherwise 0
        :rtype: int

        """
        if self.regex_pattern is not None:
            str_value = self.strvar.get().strip()
            if len(str_value) > 0:
                groups = self.apply_regex()
                if groups is not None and len(groups) == 1:
                    return 1
                else:
                    return 0
            else:
                return 1
        else:
            return 1

    def get_value(self):
        """
        Return the widget's entry value

        :return: the widget's entry value
        :rtype: str

        """
        return self.strvar.get()

    def set_value(self, value: str):
        """
        The argument value is filtered through the validation regex and the resulting regex group is used to set the
        widget's entry value

        :param value: the value to be used to set the entry value
        :type value: str
        :return: None

        """
        if self.regex_pattern is not None:
            groups = self.apply_regex(value)
            if groups is None:
                raise ValueError(f'{value} is not a valid {self.label_text}')
            else:
                self.strvar.set(groups[0])
        else:
            self.strvar.set(value)


class LabeledTextWidget:
    """
    A Frame containing a Label and a TextWidget

    """
    def __init__(self, parent, label_text: str, label_width: int, label_grid_args: dict[str, Any],
                 entry_width: int, entry_grid_args: dict[str, Any], regex_str: Optional[str] = '\\s*(\\w*)\\s*'):
        """
        Creates and instance of LabeledTextWidget

        :param parent: The GUI parent for this Frame
        :param label_text: the text to be used in creating the label
        :type label_text: str
        :param label_width: the width to be used in creating the label
        :type label_width: int
        :param label_grid_args: the arguments to be used in gridding the label
        :type label_grid_args: dict[str, Any]
        :param entry_width: the width to be used in creating the entry widget
        :type entry_width: int
        :param entry_grid_args: the arguments to be used in gridding the entry widet
        :type entry_grid_args: dict[str, Any]
        :param regex_str: the regular expression to be used for validation of input: default '\\s*(\\w*)\\s*'
        :type regex_str: str

        """
        #ttkb.Frame.__init__(self, master=parent)
        anchor = tk.W
        if 'row' in label_grid_args and 'row' in entry_grid_args and 'sticky' not in entry_grid_args:
            if label_grid_args['row'] == entry_grid_args['row']:
                entry_grid_args['sticky'] = tk.W
                anchor = tk.E
        self.label = ttkb.Label(master=parent, text=label_text, width=label_width, anchor=anchor)
        self.label.grid(**label_grid_args)
        self.entry = TextWidget(parent=parent, label_text=label_text, entry_width=entry_width, regex_str=regex_str)
        self.entry.grid(**entry_grid_args)


    def focus_set(self):
        """
        Delegates calls to focus_set to the entry widget

        :return: None

        """
        self.entry.focus_set()

    def bind(self, sequence: str | None = ...,
             func: Callable[[tk.Event], Any] | None = ...,
             add: Literal["", "+"] | bool | None = ..., ) -> str:
        """
        Delegate bind calls to the entry widget

        :param sequence: the name of the event to be bound
        :type sequence: str | None
        :param func: the callback to be invoked when the event is captured
        :type func: Callable[[tk.Event], Any]
        :param add: allows multiple bindings for single event
        :type add: Literal["", "+"] | bool | None
        :return: None

        """
        return self.entry.bind(sequence, func, add)

    def get_value(self) -> str:
        """
        Return the widget's entry value

        :return: the widget's entry value
        :rtype: str

        """
        return self.entry.get_value()

    def set_value(self, value: str) -> None:
        """
        The argument value is filtered through the validation regex and the resulting regex group is used to set the
        widget's entry value

        :param value: the value to be used to set the entry value
        :type value: str
        :return: None

        """
        self.entry.set_value(value)

    @staticmethod
    def label_width(text: str, min_width: int) -> int:
        """
        Calculates a width for the label based on the label text and a minumum width

        :param text: the label test
        :type text: str
        :param min_width: the minimum label width
        :type min_width: int
        :return: the calculated label width
        :rtype: int

        """
        text_width = len(text) + 2
        if text_width < min_width:
            return min_width
        else:
            return text_width


class IntegerWidget(EntryWidget):
    def __init__(self, parent, label_text: str, regex_str: Optional[str] = '\\s*(\\d*)\\s*'):
        """
        Creates and instance of IntegerWidget

        :param parent: the GUI parent of this widget
        :param label_text: the text for the label that will be associated with the widget.  The label is not
            created with the entry widget, but the label text is used to create ValueError exception messages
        :type label_text: str
        :param regex_str: The regular expression used to validate data entered: default '\\s*(\\d*)\\s*'
        :type regex_str: str

        """
        EntryWidget.__init__(self, parent=parent, label_text=label_text, entry_width=10, regex_str=regex_str)

    def validate(self):
        """
        A validation callback for the validationcommand parameter of the tkinter Entry widget

        :return: 1 if validate, otherwise 0
        :rtype: int

        """
        str_value = self.strvar.get().strip()
        if len(str_value) > 0:
            groups = self.apply_regex()
            if groups is not None and len(groups) == 1:
                if groups[0].isnumeric():
                    return 1
                else:
                    return 0
            else:
                return 0
        else:
            return 1

    def get_value(self):
        """
        Return the widget's entry value

        :return: the widget's entry value as an integer
        :rtype: int

        """
        value = self.strvar.get().strip()
        if len(value) == 0:
            return 0
        else:
            return int(self.strvar.get().strip())

    def set_value(self, value: Union[int, str]):
        """
        The argument value is filtered through the validation regex and the resulting regex group is used to set the
        widget's entry value

        :param value: the value to be used to set the entry value
        :type value: Union[int, str]
        :return: None

        """
        if isinstance(value, int):
            self.strvar.set(f'{value:d}')
        elif isinstance(value, str) and value.isnumeric():
            if len(value.strip()) > 0:
                re_groups = self.apply_regex(value)
                int_value: int = int(re_groups[0])
                self.strvar.set(f'{int_value:d}')
            else:
                self.strvar.set('0')
        else:
            raise ValueError(f'{value} is not a valid {self.label_text}')


class LabeledIntegerWidget:
    def __init__(self, parent, label_text: str, label_width: int, label_grid_args: dict[str, Any],
                 entry_width: int, entry_grid_args: dict[str, Any], regex_str: Optional[str] = '\\s*(\\d*)\\s*'):
        """
        Creates and instance of LabeledIntegerWidget

        :param parent: The GUI parent for this Frame
        :param label_text: the text to be used in creating the label
        :type label_text: str
        :param label_width: the width to be used in creating the label
        :type label_width: int
        :param label_grid_args: the arguments to be used in gridding the label
        :type label_grid_args: dict[str, Any]
        :param entry_width: the width to be used in creating the entry widget
        :type entry_width: int
        :param entry_grid_args: the arguments to be used in gridding the entry widet
        :type entry_grid_args: dict[str, Any]
        :param regex_str: the regular expression to be used for validation of input: default value '\\s*(\\d*)\\s*'
        :type regex_str: str

        """
        #ttkb.Frame.__init__(self, master=parent)
        row_str: str  = 'row'
        sticky_str: str = 'sticky'
        anchor = tk.W
        if row_str in label_grid_args and row_str in entry_grid_args and sticky_str not in entry_grid_args:
            if label_grid_args[row_str] == entry_grid_args[row_str]:
                anchor = tk.E
                entry_grid_args[sticky_str] = tk.W
        self.label = ttkb.Label(master=parent, text=label_text, width=label_width, anchor=anchor)
        self.label.grid(**label_grid_args)
        self.entry = IntegerWidget(parent=parent, label_text=label_text, regex_str=regex_str)
        self.entry.grid(**entry_grid_args)

    def focus_set(self):
        """
        Delegates calls to focus_set to the entry widget

        :return: None

        """
        self.entry.focus_set()

    def bind(self, sequence: str | None = ...,
             func: Callable[[tk.Event], Any] | None = ...,
             add: Literal["", "+"] | bool | None = ..., ) -> str:
        """
        Delegate bind calls to the entry widget

        :param sequence: the name of the event to be bound
        :type sequence: str | None
        :param func: the callback to be invoked when the event is captured
        :type func: Callable[[tk.Event], Any]
        :param add: allows multiple bindings for single event
        :type add: Literal["", "+"] | bool | None
        :return: None

        """
        return self.entry.bind(sequence, func, add)

    def get_value(self) -> int:
        """
        Return the widget's entry value

        :return: the widget's entry value as an integer
        :rtype: int

        """
        return self.entry.get_value()

    def set_value(self, value: Union[int, str]) -> None:
        """
        The argument value is filtered through the validation regex and the resulting regex group is used to set the
        widget's entry value

        :param value: the value to be used to set the entry value
        :type value: Union[int, str]
        :return: None

        """
        self.entry.set_value(value)

    @staticmethod
    def label_width(text: str, min_width: int) -> int:
        """
        Calculates a width for the label based on the label text and a minumum width

        :param text: the label test
        :type text: str
        :param min_width: the minimum label width
        :type min_width: int
        :return: the calculated label width
        :rtype: int

        """
        text_width = len(text) + 2
        if text_width < min_width:
            return min_width
        else:
            return text_width


class DecimalWidget(EntryWidget):
    def __init__(self, parent, label_text: str, regex_str: Optional[str] = '\\s*(\\d+[.]*\\d*)\\s*'):
        """
        Creates and instance of DecimalWidget

        :param parent: the GUI parent of this widget
        :param label_text: the text for the label that will be associated with the widget.  The label is not
            created with the entry widget, but the label text is used to create ValueError exception messages
        :type label_text: str
        :param regex_str: The regular expression used to validate data entered: default '\\s*(\\d+[.]*\\d*)\\s*'
        :type regex_str: str

        """
        EntryWidget.__init__(self, parent=parent, label_text=label_text, entry_width=10, regex_str=regex_str)

    def validate(self):
        """
        A validation callback for the validationcommand parameter of the tkinter Entry widget

        :return: 1 if validate, otherwise 0
        :rtype: int

        """
        str_value = self.strvar.get().strip()
        if len(str_value) > 0:
            groups = self.apply_regex()
            if groups is not None and len(groups) == 1:
                return 1
            else:
                return 0
        else:
            return 1

    def get_value(self) -> Decimal:
        """
        Return the widget's entry value

        :return: the widget's entry value as a Decimal
        :rtype: decimal.Decimal

        """
        if self.validate() == 1:
            str_value = self.strvar.get().strip()
            if len(str_value) == 0:
                return Decimal(0)
            else:
                return Decimal(str_value)
        else:
            raise ValueError(f'{self.strvar.get()} is not a valid {self.label_text}')

    def set_value(self, value: Union[Decimal, str]):
        """
        The argument value is filtered through the validation regex and the resulting regex group is used to set the
        widget's entry value

        :param value: the value to be used to set the entry value
        :type value: Union[decimal.Decimal, str]
        :return: None

        """
        if isinstance(value, Decimal):
            self.strvar.set(f'{value:.1f}')
        elif isinstance(value, str) and value.isnumeric():
            if len(value.strip()) > 0:
                re_groups = self.apply_regex()
                if re_groups is not None:
                    dec_value: Decimal = Decimal(re_groups[0])
                    self.strvar.set(f'{dec_value:.1f}')
                raise ValueError(f'{self.strvar.get()} is not a valid {self.label_text}')
            else:
                self.strvar.set('0.0')
        else:
            raise ValueError(f'{self.strvar.get()} is not a valid {self.label_text}')


class LabeledDecimalWidget:
    def __init__(self, parent, label_text: str, label_width: int, label_grid_args: dict[str, Any],
                 entry_width: int, entry_grid_args: dict[str, Any], regex_str: Optional[str] = '\\s*(\\d+[.]*\\d*)\\s*'):
        """
        Creates and instance of LabeledDecimalWidget

        :param parent: The GUI parent for this Frame
        :param label_text: the text to be used in creating the label
        :type label_text: str
        :param label_width: the width to be used in creating the label
        :type label_width: int
        :param label_grid_args: the arguments to be used in gridding the label
        :type label_grid_args: dict[str, Any]
        :param entry_width: the width to be used in creating the entry widget
        :type entry_width: int
        :param entry_grid_args: the arguments to be used in gridding the entry widet
        :type entry_grid_args: dict[str, Any]
        :param regex_str: the regular expression to be used for validation of input: default value '\\s*(\\d+[.]*\\d*)\\s*'
        :type regex_str: str

        """
        #ttkb.Frame.__init__(self, master=parent)
        row_str: str = 'row'
        sticky_str: str = 'sticky'
        anchor = tk.W
        if row_str in label_grid_args and row_str in entry_grid_args and sticky_str not in entry_grid_args:
            if label_grid_args[row_str] == entry_grid_args[row_str]:
                anchor = tk.E
                entry_grid_args[sticky_str] = tk.W
        self.label = ttkb.Label(parent, text=label_text, width=label_width, anchor=anchor)
        self.label.grid(**label_grid_args)
        self.entry = DecimalWidget(parent=parent, label_text=label_text, regex_str=regex_str)
        self.entry.grid(**entry_grid_args)

    def focus_set(self):
        """
        Delegates calls to focus_set to the entry widget

        :return: None

        """
        self.entry.focus_set()

    def bind(self, sequence: str | None = ...,
             func: Callable[[tk.Event], Any] | None = ...,
             add: Literal["", "+"] | bool | None = ..., ) -> str:
        """
        Delegate bind calls to the entry widget

        :param sequence: the name of the event to be bound
        :type sequence: str | None
        :param func: the callback to be invoked when the event is captured
        :type func: Callable[[tk.Event], Any]
        :param add: allows multiple bindings for single event
        :type add: Literal["", "+"] | bool | None
        :return: None

        """
        return self.entry.bind(sequence, func, add)

    def get_value(self) -> Decimal:
        """
        Return the widget's entry value

        :return: the widget's entry value as a Decimal
        :rtype: decimal.Decimal

        """
        return self.entry.get_value()

    def set_value(self, value: Union[Decimal, str]) -> None:
        """
        The argument value is filtered through the validation regex and the resulting regex group is used to set the
        widget's entry value

        :param value: the value to be used to set the entry value
        :type value: Union[decimal.Decimal, str]
        :return: None

        """
        self.entry.set_value(value)

    @staticmethod
    def label_width(text: str, min_width: int) -> int:
        """
        Calculates a width for the label based on the label text and a minumum width

        :param text: the label test
        :type text: str
        :param min_width: the minimum label width
        :type min_width: int
        :return: the calculated label width
        :rtype: int

        """
        text_width = len(text) + 2
        if text_width < min_width:
            return min_width
        else:
            return text_width


class WeekDayCheckbuttonWidget(ttkb.Frame):
    """
    A Frame containing a checkbox for each day of the week, allows selection of a list of week days
    """
    def __init__(self, parent):
        """
        Creates and instance of WeekDayCheckbuttonWidget

        :param parent: the GUI parent of the frame

        """
        ttkb.Frame.__init__(self, parent)
        self.day_vars: dict[dp.WeekDay, ttkb.IntVar] = {}
        self.check_buttons: list[Checkbutton] = []
        row = 0
        for day in dp.WeekDay:
            day_var = ttkb.IntVar()
            day_var.set(0)
            button = Checkbutton(self, text=day.name, variable=day_var, width=15)
            button.grid(column=0, row=row, sticky=NW, padx=5, pady=1)
            self.day_vars[day] = day_var
            self.check_buttons.append(button)
            row += 1
        self.grid()

    def disable(self):
        for button in self.check_buttons:
            button.configure(state=DISABLED)

    def enable(self):
        for button in self.check_buttons:
            button.configure(state=NORMAL)

    def get_checked_days(self) -> list[dp.WeekDay]:
        """
        Returns a list of WeekDay enums for the days whose checkboxes where selected

        :return:  a list of WeeKDay enums
        :rtype: list[biometrics_tracker.model.datapoints.WeekDay]
        """
        days: list[dp.WeekDay] = []
        for day, var in self.day_vars.items():
            if var.get() > 0:
                days.append(day)
        return days

    def set_checked_days(self, days: list[dp.WeekDay]) -> None:
        for day, var in self.day_vars.items():
            if day not in days:
                var.set(0)
            else:
                var.set(1)


class DateWidget(ttkb.Frame):
    """
    A general purpose date entry widget
    """
    def __init__(self, parent, default_value: datetime.date = None):
        """
        Creates widgets.DateWidget instance
        :param parent: the GUI element that will contain the created widget
        :param default_value: a datetime.date value to be used as an initial value for the month, day and year
        entry fields.
        """
        ttkb.Frame.__init__(self, parent)
        self.month_var = ttkb.StringVar()
        self.day_var = ttkb.StringVar()
        self.year_var = ttkb.StringVar()
        self.prev_entry = None
        self.next_entry = None
        if default_value:
            self.month_var.set(default_value.month)
            self.day_var.set(default_value.day)
            self.year_var.set(default_value.year)
        self.month_entry = ttkb.Entry(self, textvariable=self.month_var, width=3)
        self.month_entry.grid(column=0, row=0, sticky=tk.NW, ipadx=0, padx=0)
        ttkb.Label(self, text="/", width=1, font=('courier', 20, 'bold')).grid(column=1, row=0, sticky=tk.NW, padx=0,
                                                                               pady=5)
        self.month_entry.bind('<KeyPress>', self.month_keypress)
        self.month_entry.bind('<FocusIn>', self.clear_key_count)
        add_validation(self.month_entry, month_validator)
        self.day_entry = ttkb.Entry(self, textvariable=self.day_var, width=3)
        self.day_entry.grid(column=2, row=0, sticky=tk.NW)
        self.day_entry.bind('<KeyPress>', self.day_keypress)
        self.day_entry.bind('<FocusIn>', self.clear_key_count)
        add_validation(self.day_entry, day_validator)
        ttkb.Label(self, text="/", width=1, font=('courier', 20, 'bold')).grid(column=3, row=0, sticky=tk.NW, padx=0,
                                                                               pady=5)
        self.year_entry = ttkb.Entry(self, textvariable=self.year_var, width=6)
        self.year_entry.grid(column=4, row=0, stick=tk.NW)
        self.year_entry.bind('<KeyPress>', self.year_keypress)
        self.year_entry.bind('<FocusIn>', self.clear_key_count)
        self.year_entry.bind('<FocusOut>', self.validate_date)
        add_validation(self.year_entry, year_validator)
        self.grid()
        self.key_count = 0
        self.max_dom: dict[int, int] = {
            1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
        }
        self.prev_key_press = None
        self.error: bool = False

    def set_prev_entry(self, entry) -> None:
        """
        Establishes the widget to focus on when the Back Tab key is pressed in the Month field

        :param entry:
        :return: None

        """
        self.prev_entry = entry

    def set_next_entry(self, entry) -> None:
        """
        Establishes the widget to focus on when the Tab key is pressed in the Year field

        :param entry:
        :return: None

        """
        self.next_entry = entry

    def focus_set(self) -> None:
        """
        Delegates focus_set to the month entry

        :return: None

        """
        self.month_entry.focus_set()
        self.key_count = 0
        self.prev_key_press = None

    def disable(self) -> None:
        """
        Disables the month, day and year fields

        :return: None

        """
        self.month_entry.configure(state=DISABLED)
        self.day_entry.configure(state=DISABLED)
        self.year_entry.configure(state=DISABLED)

    def enable(self) -> None:
        """
        Enables the month, day and year fields

        :return: None

        """
        self.month_entry.configure(state=NORMAL)
        self.day_entry.configure(state=NORMAL)
        self.year_entry.configure(state=NORMAL)

    def select_range(self, start, end):
        """
        Delegates select range functionality to the month entry field
        :param start:
        :param end:
        :return:
        """
        self.month_entry.select_range(start, end)

    def clear_key_count(self, event):
        """
        Sets the keystroke counter to 0.  This method is bound to the <FocusIn> event for the month, day and year
        fields
        :param event:
        :return: None
        """
        self.key_count = 0

    def month_keypress(self, event):
        """
        Tracks the keystrokes entered to the month field, puts the day entry field in focus after two keystrokes.
        This method is bound to the <KeyPress> event of the month field.
        :param event:
        :return: None
        """
        if event.char.isnumeric():
            self.key_count += 1
            if self.key_count == 2:
                self.key_count = 0
                self.day_entry.select_range(0, END)
                self.day_entry.focus_set()
            self.prev_key_press = None
        else:
            match event.keysym:
                case 'Tab':
                    self.key_count = 0
                case 'ISO_Left_Tab':
                    self.key_count = 0
                    if self.prev_entry is not None:
                        self.prev_entry.focus_set()
                case 'BackSpace':
                    if self.key_count > 0:
                        self.key_count -= 1
                case 'Up':
                    m = int(self.month_var.get())
                    if m < 12:
                        self.month_var.set(str(m + 1))
                    else:
                        self.month_var.set('01')
                    self.month_entry.update()
                case 'KP_Up':
                    m = int(self.month_var.get())
                    if m < 12:
                        self.month_var.set(str(m + 1))
                    else:
                        self.month_var.set('01')
                    self.month_entry.update()
                case 'Down':
                    m = int(self.month_var.get())
                    if m > 1:
                        self.month_var.set(str(m - 1))
                    else:
                        self.month_var.set('12')
                    self.month_entry.update()
                case 'KP_Down':
                    m = int(self.month_var.get())
                    if m > 1:
                        self.month_var.set(str(m - 1))
                    else:
                        self.month_var.set('12')
                    self.month_entry.update()

            self.prev_key_press = event.keysym

    def day_keypress(self, event):
        """
        Tracks the keystrokes entered to the day field, puts the year entry field in focus after two keystrokes.
        This method is bound to the <KeyPress> event of the day entry field.
        :param event:
        :return: None
        """
        if event.char.isnumeric():
            self.key_count += 1
            if self.key_count == 2:
                self.key_count = 0
                self.year_entry.select_range(0, END)
                self.year_entry.focus_set()
            self.prev_key_press = None
        else:
            match event.keysym:
                case 'Tab':
                    self.key_count = 0
                case 'ISO_Left_Tab':
                    self.key_count = 0
                case 'BackSpace':
                    if self.key_count > 0:
                        self.key_count -= 1
                case 'Up':
                    m = int(self.day_var.get())
                    if m < self.max_dom[int(self.month_var.get())]:
                        self.day_var.set(str(m + 1))
                    else:
                        self.day_var.set('01')
                    self.day_entry.update()
                case 'KP_Up':
                    m = int(self.day_var.get())
                    if m < self.max_dom[int(self.month_var.get())]:
                        self.day_var.set(str(m + 1))
                    else:
                        self.day_var.set('01')
                    self.day_entry.update()
                case 'Down':
                    m = int(self.day_var.get())
                    if m > 1:
                        self.day_var.set(str(m - 1))
                    else:
                        self.day_var.set(str(self.max_dom[int(self.month_var.get())]))
                    self.day_entry.update()
                case 'KP_Down':
                    m = int(self.day_var.get())
                    if m > 1:
                        self.day_var.set(str(m - 1))
                    else:
                        self.day_var.set(str(self.max_dom[int(self.month_var.get())]))
                    self.day_entry.update()

            self.prev_key_press = event.keysym

    def year_keypress(self, event):
        """
        Tracks the keystrokes entered to the year field, puts the next entry field on the GUI in focus after two
        keystrokes. This method is bound to the <KeyPress> event of the year entry field.
        :param event:
        :return: None
        """
        if event.char.isnumeric():
            self.key_count += 1
            if self.key_count == 4:
                self.key_count = 0
                self.next_entry.focus_set()
            self.prev_key_press = None
        else:
            match event.keysym:
                case 'Tab':
                    self.key_count = 0
                    self.next_entry.focus_set()
                case 'ISO_Left_Tab':
                    self.key_count = 0
                case 'BackSpace':
                    if self.key_count > 0:
                        self.key_count -= 1
                case 'Up':
                    m = int(self.year_var.get())
                    self.year_var.set(str(m + 1))
                    self.year_entry.update()
                case 'KP_Up':
                    m = int(self.year_var.get())
                    self.year_var.set(str(m + 1))
                    self.year_entry.update()
                case 'Down':
                    m = int(self.year_var.get())
                    if m > 0:
                        self.year_var.set(str(m - 1))
                        self.year_entry.update()
                case 'KP_Down':
                    m = int(self.year_var.get())
                    if m > 1:
                        self.year_var.set(str(m - 1))
                        self.year_entry.update()

            self.prev_key_press = event.keysym

    def validate_date(self, event):
        """
        Validates the month, day and year elements of the date and presents a Messagebox if any is invalid
        This method is bound to the Focus Out event of the year entry field.
        :param event:
        :return: None
        """
        if self.prev_key_press != 'ISO_Left_Tab':
            valid = True
            if self.month_var.get().isnumeric():
                month: int = int(self.month_var.get())
                if not 1 <= month <= 12:
                    self.month_var.set('')
                    self.month_entry.update()
                    self.month_entry.focus_set()
                    valid = False
                if valid:
                    if self.day_var.get().isnumeric():
                        try:
                            day: int = int(self.day_var.get())
                            if not 1 <= day <= self.max_dom[month]:
                                self.day_var.set('')
                                self.day_entry.update()
                                self.day_entry.focus_set()
                                valid = False
                        except KeyError:
                            # This should never happen, as the month has already been validated
                            pass
                        if valid and not self.year_var.get().isnumeric():
                            self.year_var.set('')
                            self.year_entry.update()
                            self.year_entry.focus_set()
                            valid = False
                    else:
                        # If the day is not numeric
                        self.day_var.set('')
                        self.day_entry.update()
                        self.day_entry.focus_set()
                        valid = False
            else:
                # If the month is not numeric
                self.month_var.set('')
                self.month_entry.update()
                self.month_entry.focus_set()
                valid = False
            if valid and int(self.year_var.get()) < 100:
                year = 2000 + int(self.year_var.get())
                self.year_var.set(f'{year:4d}')
                self.year_entry.update()
            if valid:
                self.next_entry.focus_set()
                self.error = False
            else:
                if not self.error:
                    self.error = True
                    dialogs.Messagebox.ok("Date entered is not valid.", "Date Entry Error")

    def get_date(self) -> Optional[date]:
        """
        Build a date value from the values entered to the month, day and year entry fields.

        :return: datetime.date instance

        """
        try:
            month: int = int(self.month_var.get())
            day: int = int(self.day_var.get())
            year: int = int(self.year_var.get())
            return date(year=year, month=month, day=day)
        except ValueError:
            return None

    def set_date(self, date_value: date):
        """
        Set the month, day and year field from the provided date

        :param date_value: the new value for the date widget
        :type date_value: date
        :return: None

        """
        self.month_var.set(str(date_value.month))
        self.day_var.set(str(date_value.day))
        self.year_var.set(str(date_value.year))


class TimeWidget(ttkb.Frame):
    """
    A general purpose date entry widget
    """
    def __init__(self, parent, default_value: time = None):
        """
        Creates widgets.TimeWidget instance
        :param parent: the GUI element that will contain the created widget
        :param default_value: a datetime.time value to be used as an initial value for the hour and minute
        entry fields.
        """
        ttkb.Frame.__init__(self, parent)
        self.columnconfigure(0, weight=2)
        self.columnconfigure(1, weight=1)
        self.columnconfigure(2, weight=2)
        self.columnconfigure(3, weight=2)
        self.hour_var = ttkb.StringVar()
        self.minute_var = ttkb.StringVar()
        self.ampm_var = ttkb.StringVar()
        self.prev_entry = None
        self.next_entry = None
        if default_value is not None:
            fmt_time = default_value.strftime('%I:%M %p')
            self.hour_var.set(fmt_time[0:2])
            self.minute_var.set(fmt_time[3:5])
            self.ampm_var.set(fmt_time[6:8])
        self.hour_entry = ttkb.Entry(self, textvariable=self.hour_var, width=3)
        self.hour_entry.grid(column=0, row=0, sticky=tk.W, ipadx=0, padx=0, pady=5)
        ttkb.Label(self, text=":", width=1, font=('courier', 20, 'bold')).grid(column=1, row=0, sticky=tk.NW, padx=0,
                                                                               pady=5)
        self.hour_entry.bind('<KeyPress>', self.hour_keypress)
        self.hour_entry.bind('<FocusIn>', self.clear_key_count)
        add_validation(self.hour_entry, hour_validator)
        self.minute_entry = ttkb.Entry(self, textvariable=self.minute_var, width=3)
        self.minute_entry.grid(column=2, row=0, sticky=tk.W, ipadx=0, padx=0, pady=5)
        self.minute_entry.bind('<KeyPress>', self.minute_keypress)
        self.minute_entry.bind('<FocusIn>', self.clear_key_count)
        add_validation(self.minute_entry, minute_validator)
        self.am_button = Radiobutton(self, text='AM', value='AM', variable=self.ampm_var, command=self.validate_time)
        self.am_button.grid(column=3, row=0, stick=tk.W, padx=5, pady=5)
        self.pm_button = Radiobutton(self, text='PM', value='PM', variable=self.ampm_var, command=self.validate_time)
        self.pm_button.bind('<FocusOut>', self.validate_time, '+')
        self.pm_button.grid(column=4, row=0, sticky=tk.W, padx=5, pady=5)
        if self.ampm_var.get() not in ['AM', 'PM']:
            self.ampm_var.set('AM')
        self.grid()
        self.key_count = 0
        self.prev_key_press = None
        self.error: bool = False

    def disable(self) -> None:
        """
        Disables the hour and minute fields and the ap/pm radio buttons.

        :return: None

        """
        self.hour_entry.configure(state=DISABLED)
        self.minute_entry.configure(state=DISABLED)
        self.am_button.configure(state=DISABLED)
        self.pm_button.configure(state=DISABLED)

    def enable(self) -> None:
        """
        Enables the hour and minute fields and the am/pm radio buttons.

        :return: None

        """
        self.hour_entry.configure(state=NORMAL)
        self.minute_entry.configure(state=NORMAL)
        self.am_button.configure(state=NORMAL)
        self.pm_button.configure(state=NORMAL)

    def set_prev_entry(self, entry) -> None:
        """
        Establishes the widget that will receive focus when the Back Tab key is pressed in the hour field.

        :param entry:
        :return: None

        """
        self.prev_entry = entry

    def set_next_entry(self, entry):
        """
        Establishes the widget that will receive focus after the am/pm radio buttons

        :param entry:
        :return: None

        """
        self.next_entry = entry

    def focus_set(self) -> None:
        """
        Delegates set focus functionality to the hour field

        :return: None
        """
        self.hour_entry.focus_set()
        self.key_count = 0
        self.prev_key_press = None

    def select_range(self, start, end) -> None:
        """
        Delegates select range to the hour field

        :param start: start position for selection
        :param end: end position for selection
        :return: None

        """
        self.hour_entry.select_range(start, end)

    def clear_key_count(self, event):
        """
        Sets the keystroke counter to 0.  This method is bound to the <FocusIn> event for the hour and minute entry
        fields
        :param event:
        :return: None
        """
        self.key_count = 0

    def hour_keypress(self, event):
        """
        Tracks the keystrokes entered to the hour field, puts the minute entry field on the GUI in focus after two
        keystrokes. This method is bound to the <KeyPress> event of the hour entry field.
        :param event:
        :return: None
        """
        if event.char.isnumeric():
            self.key_count += 1
            if self.key_count == 2:
                self.key_count = 0
                self.minute_entry.select_range(0, END)
                self.minute_entry.focus_set()
            self.prev_key_press = None
        else:
            match event.keysym:
                case 'Tab':
                    self.key_count = 0
                case 'ISO_Left_Tab':
                    self.key_count = 0
                    if self.prev_entry is not None:
                        self.prev_entry.select_range(0, END)
                        self.prev_entry.focus_set()
                case 'BackSpace':
                    if self.key_count > 0:
                        self.key_count -= 1
                case 'Up':
                    h = int(self.hour_var.get())
                    if h < 12:
                        self.hour_var.set(str(h + 1))
                    else:
                        self.hour_var.set('01')
                    self.hour_entry.update()
                case 'KP_Up':
                    h = int(self.hour_var.get())
                    if h < 12:
                        self.hour_var.set(str(h + 1))
                    else:
                        self.hour_var.set('01')
                    self.hour_entry.update()
                case 'Down':
                    h = int(self.hour_var.get())
                    if h > 1:
                        self.hour_var.set(str(h - 1))
                    else:
                        self.hour_var.set('12')
                    self.hour_entry.update()
                case 'KP_Down':
                    h = int(self.hour_var.get())
                    if h > 1:
                        self.hour_var.set(str(h - 1))
                    else:
                        self.hour_var.set('12')
                    self.hour_entry.update()

            self.prev_key_press = event.keysym

    def minute_keypress(self, event):
        """
        Tracks the keystrokes entered to the minute field, puts the next entry field on the GUI  on the GUI in focus
        after two keystrokes. This method is bound to the <KeyPress> event of the hour entry field.
        :param event:
        :return: None
        """
        if event.char.isnumeric():
            self.key_count += 1
            if self.key_count == 2:
                self.key_count = 0
                self.am_button.focus_set()
            self.prev_key_press = None
        else:
            match event.keysym:
                case 'Tab':
                    self.key_count = 0
                case 'ISO_Left_Tab':
                    self.key_count = 0
                case 'BackSpace':
                    if self.key_count > 0:
                        self.key_count -= 1
                case 'Up':
                    m = int(self.minute_var.get())
                    if m < 59:
                        self.minute_var.set(str(m + 1))
                        self.minute_entry.update()
                    else:
                        self.minute_var.set('00')
                case 'KP_Up':
                    m = int(self.minute_var.get())
                    if m < 59:
                        self.minute_var.set(str(m + 1))
                    else:
                        self.minute_var.set('00')
                    self.minute_entry.update()
                case 'Down':
                    m = int(self.minute_var.get())
                    if m > 1:
                        self.minute_var.set(str(m - 1))
                    else:
                        self.minute_var.set('59')
                    self.minute_entry.update()
                case 'KP_Down':
                    m = int(self.minute_var.get())
                    if m > 1:
                        self.minute_var.set(str(m - 1))
                    else:
                        self.minute_var.set('59')
                    self.minute_entry.update()


            self.prev_key_press = event.keysym

    def am_keypress(self, event):
        """
        If the Tab key is pressed when the AM radio button is in focus, skip over the PM button and set the
        focus on the next_entry field if any has been specified
        :param event:
        :return: None
        """
        if event.keysym == 'Tab' and self.next_entry is not None:
            self.next_entry.focus_set()

    def validate_time(self, event=None):
        """
        Validate the hour and minute values, display a MessageBox if either is invalid. This method is bound
        to the Button-1 events on the AM and PM radio buttons
        :param event:
        :return: None
        """
        if self.prev_key_press != 'ISO_Left_Tab':
            valid = True
            if self.hour_entry.get().isnumeric():
                hour = int(self.hour_var.get())
                if not 1 <= hour <= 12:
                    self.hour_var.set('')
                    self.hour_entry.select_range(0, END)
                    self.hour_entry.focus_set()
                    valid = False
                if valid:
                    if self.minute_entry.get().isnumeric():
                        minute = int(self.minute_var.get())
                        if not 0 <= minute <= 60:
                            self.minute_var.set('')
                            self.minute_entry.select_range(0, END)
                            self.minute_entry.focus_set()
                            valid = False
                    else:
                        self.minute_var.set('')
                        self.minute_entry.select_range(0, END)
                        self.minute_entry.focus_set()
                        valid = False
            else:
                self.hour_var.set('')
                self.hour_entry.select_range(0, END)
                self.hour_entry.focus_set()
                valid = False
            if valid:
                if self.next_entry is not None:
                    self.next_entry.focus_set()
                self.error = False
            else:
                if not self.error:
                    dialogs.Messagebox.ok("Time entered is not valid.", "Time Entry Error")
                    self.error = True

    def get_time(self) -> Optional[time]:
        """
        Builds a datetime.time instance using the values entered to the hour and minute entry fields.
        :return: datetime.time instance
        """
        if len(self.hour_var.get().strip()) == 0 or len(self.minute_var.get().strip()) == 0:
            return None
        else:
            hour: int = int(self.hour_var.get())
            minute: int = int(self.minute_var.get())
            ampm: str = self.ampm_var.get()
            if ampm == "PM":
                if hour < 12:
                    hour += 12
            return time(hour=hour, minute=minute)

    def get_datetime(self):
        """
        Returns a datetime instance with a zero date.

        :return: datetime instance
        :rtype: datetime

        """
        dummy = date(year=2022, month=10, day=23)
        return utilities.mk_datetime(dummy, self.get_time())

    def set_time(self, time_value: time) -> None:
        """
        Set the hour, minute and am/pm fields to the provided time

        :param time_value: the new value for the time widget
        :type time_value: time
        :return: None

        """
        hour: int = time_value.hour
        minute: int = time_value.minute
        ampm: str = 'AM'
        if hour >= 12:
            hour -= 12
            ampm = 'PM'
        self.hour_var.set(str(hour))
        self.minute_var.set(str(minute))
        self.ampm_var.set(ampm)

    def set_datetime(self, dt_value: datetime):
        dt, tm = utilities.split_datetime(dt_value)
        self.set_time(tm)


class UOMListWidget(tk.Listbox):
    """
    A widget that extends tk.Listbox to present a selection list of u.o.m. Enum values for one of the Enum classes
    defined in model.uoms
    """
    def __init__(self, parent, uom_enum: uoms.UOM, selected_uom: uoms.UOM):
        """
        Creates a UOMListWidget instance
        :param parent: the widget's GUI parent element
        :param uom_enum: the Enum whose values will be used to populate the Listbox
        :param selected_uom: the Enum value that will be initial selected
        """
        tk.Listbox.__init__(self, parent)
        self.config(selectmode=tk.SINGLE, height=len(list(uom_enum)) + 1)
        self.uom_enum: uoms.UOM = uom_enum
        self.selected_uom: uoms.UOM = selected_uom
        self.sel_list = []
        for idx, sel in enumerate(list(self.uom_enum)):
            self.insert(idx, sel)
            self.sel_list.append(sel)
            if sel == selected_uom:
                self.select_set(idx, idx)
        self.bind('<Double-1>', self.set_selected)

    def set_selected(self):
        """
        Retrieves the Listbox's selected element and uses it to set the selection_uom property.  This method is bound
        to the <Double-1> event for the Listbox
        :return: None
        """
        idx, = self.curselection()
        self.selected_uom = self.sel_list[idx]


class UOMRadioWidget(ttkb.Frame):
    """
    A widget that contains a series of ttkbootstrap.Radiobutton instances based on u.o.m. Enum values for one of the
     Enum classes defined in model.uoms
    """
    def __init__(self, parent, uom_enum: uoms.UOM, selected_uom=None):
        ttkb.Frame.__init__(self, parent)
        self.columnconfigure("all", weight=1)
        self.uom_enum: uoms.UOM = uom_enum
        self.selected_uom: uoms.UOM = selected_uom
        self.sel_list = []
        self.uom_var = tk.StringVar()
        for idx, sel in enumerate(list(self.uom_enum)):
            rb = Radiobutton(self, text=sel, variable=self.uom_var, value=sel.name, command=self.handle_selection,
                             width=20)
            rb.grid(column=idx, row=0, sticky=NSEW)
        if selected_uom:
            uom_var = selected_uom.name
        self.grid()

    def handle_selection(self):
        """
        Retrieves the select radio button value and uses it to return the corresponding model.uoms.UOM extending
        instance
        :return: an instance of an Enum that extends model.uoms.UOM
        """
        self.selected_uom = uoms.uom_map[self.uom_var.get()]

    def set_selection(self, uom: uoms.UOM):
        """
        Selects the radio button that corresponds to the provided model.uoms.UOM extending value
        :param uom: an instance of an Enum that extends model.uoms.UOM
        :return: None
        """
        self.selected_uom = uom
        self.uom_var.set(uom.name)

    def enable(self):
        """
        Enables the radio button group for entry
        :return: None
        """
        for widget in self.children.values():
            if widget.config:
                widget.config(state=NORMAL)

    def disable(self):
        """
        Disables the radio button group for entry
        :return: None
        """
        for widget in self.children.values():
            if widget.config:
                widget.config(state=DISABLED)


class DataPointTypeWidget(ttk.Frame):
    """
    A widget that contains a checkbox and a gui.widgets.UOMRadioWidget.  The radio button group is disabled unless
    the checkbox is checked.  This widget is used on gui.tk_gui.PersonFrame to allow the selection of tracked
    metrics and a selection of the u.o.m to be used for the metric
    """
    def __init__(self, parent, dp_type: dp.DataPointType, uom: uoms.UOM, tracked: bool):
        """
        Creates an instance of gui.widgets.DataPointTypeWidget
        :param parent: the GUI parent for this widget
        :param dp_type: the model.datapoints.DataPointType to be associated with the widget
        :param uom: the model.uoms.UOM extending instance that will be used to create the gui.widgets.UOMRadioWidget
        :param tracked: boolean value, true if the DataPointType is already tracked for the associated Person
        """
        ttk.Frame.__init__(self, parent)
        self.columnconfigure("all", weight=1)
        self.dp_type = dp_type
        dp_class = dp.dptype_dp_map[dp_type]
        self.tracked_var = tk.IntVar()
        self.check_btn = Checkbutton(self, text=dp_class.label(), variable=self.tracked_var,
                                     command=self.set_tracked, padding=5, width=15)
        self.check_btn.grid(column=0, row=0)
        self.uom_widget = UOMRadioWidget(self, uom)
        self.uom_widget.grid(column=1, row=0)
        if tracked:
            self.tracked_var.set(1)
            self.uom_widget.enable()
        else:
            self.tracked_var.set(0)
            self.uom_widget.disable()
        self.tracked = tracked
        self.grid()

    def set_tracked(self):
        """
        Retrieves the variable value for the checkbox.  If the box is checked, the u.o.m radio buttons are enabled,
        otherwise they are disabled
        :return: None
        """
        if self.tracked_var.get() > 0:
            self.tracked = True
            self.uom_widget.enable()
        else:
            self.tracked = False
            self.uom_widget.disable()

    def focus_set(self) -> None:
        """
        Sets the focus on the check_btn field when the focus_set method on the DataPointTypeWidget is called

        :return: None

        """
        self.check_btn.focus_set()


class NoteTip:
    """
    Implements a tool tip type window to display the note associated with a model.datapoints.DataPoint derived
    instance
    """
    def __init__(self, widget, text):
        self.widget = widget
        self.widget.bind('<Enter>', self.showtip)
        self.widget.bind('<Leave>', self.hidetip)
        self.tipwindow = None
        self.id = None
        self.x = self.y = 0
        self.text = text

    def showtip(self, event):
        """
        Display Note tool tip
        :param: event
        :return: None
        """
        if event:
            pass
        if self.tipwindow or len(self.text) == 0 or self.text.isspace():
            return
        x, y, cx, cy = self.widget.bbox("insert")
        x = x + self.widget.winfo_rootx() + 40
        y = y + cy + self.widget.winfo_rooty() + 10
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry("+%d+%d" % (x, y))
        label = ttk.Label(tw, text=f'Note: {self.text}', relief=FLAT, borderwidth=1)
        label.grid(column=0, row=0)
        tw.grid()

    def hidetip(self, event=None):
        """
        Hide the note tool tip
        :param event
        :return:
        """
        if event:
            pass
        tw = self.tipwindow
        self.tipwindow = None
        if tw:
            tw.destroy()


class IntegerMetricWidget(LabeledIntegerWidget):
    def __init__(self, parent, dp_type: dp.DataPointType, uom: uoms.UOM, min_label_width: int, entry_width: int,
                 label_grid_args: dict[str, Any], entry_grid_args: dict[str, Any]):
        LabeledIntegerWidget.__init__(self, parent=parent,
                                      label_text=f'{dp.dptype_dp_map[dp_type].label()}:',
                                      label_width=self.label_width(dp.dptype_dp_map[dp_type].label(), min_label_width),
                                      label_grid_args=label_grid_args,
                                      entry_width=entry_width,
                                      entry_grid_args=entry_grid_args)
        ttkb.Label(parent, text=uom.abbreviation(), width=10).grid(column=entry_grid_args['column']+1,
                                                                   row=entry_grid_args['row'], sticky=tk.EW)


class DecimalMetricWidget(LabeledDecimalWidget):
    def __init__(self, parent, dp_type: dp.DataPointType, uom: uoms.UOM, min_label_width: int, entry_width: int,
                 label_grid_args: dict[str, Any], entry_grid_args: dict[str, Any]):
        LabeledDecimalWidget.__init__(self, parent=parent,
                                      label_text=f'{dp.dptype_dp_map[dp_type].label()}:',
                                      label_width=self.label_width(dp.dptype_dp_map[dp_type].label(), min_label_width),
                                      label_grid_args=label_grid_args,
                                      entry_width=entry_width,
                                      entry_grid_args=entry_grid_args)
        ttkb.Label(parent, text=uom.abbreviation(), width=10).grid(column=entry_grid_args['column']+1,
                                                                   row=entry_grid_args['row'], sticky=tk.EW)


class MetricWidget(ttkb.Frame):
    """
    A base class for widget that allow the entry of information on the recording of a particular type of metric
    """
    def __init__(self, parent, track_cfg: dp.TrackingConfig, datapoint: dp.DataPoint=None,
                 show_note_field: bool = True):
        """
        Creates a gui.widgets.DataPointWidget
        :param parent: the GUI parent for the widget
        :param track_cfg: an instance of model.datapoints.TrackingConfig to provide the u.o.m associated with the
        datapoint.
        """
        ttkb.Frame.__init__(self, parent)
        self.dp_type: dp.DataPointType = track_cfg.dp_type
        self.uom: uoms.UOM = track_cfg.default_uom
        self.datapoint: dp.DataPoint = datapoint
        self.changed: bool = False
        self.note_var: ttkb.StringVar = ttkb.StringVar()
        if datapoint:
            self.note_var.set(datapoint.note)
        self.show_note_field = show_note_field
        self.note_entry: Optional[LabeledTextWidget] = None
        self.note_tip: Optional[NoteTip] = None
        self.up_arrow_entry: Optional[MetricWidget] = None
        self.down_arrow_entry: Optional[MetricWidget] = None

    @abstractmethod
    def get_value(self):
        """
        This method must be overriden to return the value entered for the metric
        :return: metric value
        """
        ...

    @abstractmethod
    def zero_value(self) -> None:
        """
        This method must be overriden to set the IntVar associated with the value entry for the metric to zero
        and the note field to an empty string
        :return: None
        """
        ...

    @abstractmethod
    def focus_set(self) -> None:
        """
        This method must be overridden to set the focus on the value widget
        :return: None
        """

    def create_note_entry(self) -> None:
        """
        Create a note entry field either as a Entry in the DataPointFrame, or as a popup invoked a right click

        :return: None

        """
        if self.show_note_field:
            self.note_entry = LabeledTextWidget(self, label_text='Note:', label_width=6,
                                                label_grid_args={'column': 3, 'row': 0, 'sticky': tk.E, 'padx': 5,
                                                                 'pady': 5},
                                                entry_width=30,
                                                entry_grid_args={'column': 4, 'row': 0, 'sticky': tk.E, 'padx': 5,
                                                                 'pady': 5, 'columnspan': 2})
            if self.datapoint:
                self.note_entry.set_value(self.datapoint.note)
            self.note_entry.bind('<FocusOut>', self.check_changed_note)
            self.note_entry.bind('<KeyPress>', self.handle_keypress)
            self.note_var = self.note_entry.entry.get_var()
        else:
            self.note_tip = NoteTip(self, self.note_var.get())
            self.bind('<Button-3>', self.edit_note)
            for child in self.children.values():
                child.bind('<Button-3>', self.edit_note)

    def handle_keypress(self, event):
        """
        Watch for up and down arrow key presses, if caught, set focus on the DataPointWidget above or below

        :param event:
        :return: None

        """
        match event.keysym:
            case 'Up':
                if self.up_arrow_entry is not None:
                    self.up_arrow_entry.focus_set()
            case 'KP_Up':
                if self.up_arrow_entry is not None:
                    self.up_arrow_entry.focus_set()
            case 'Down':
                if self.down_arrow_entry is not None:
                    self.down_arrow_entry.focus_set()
            case 'KP_Down':
                if self.down_arrow_entry is not None:
                    self.down_arrow_entry.focus_set()
            case _:
                pass

    def check_changed_note(self, event=None):
        """
        Check to see if the note property has been changed
        :param event:
        :return: None
        """
        if event:
            pass
        if self.datapoint and self.datapoint.note.strip() != self.note_var.get().strip():
            self.changed = True

    def is_changed(self) -> bool:
        """
        Returns a boolean flag that indicates whether the widgets value property has been changed.  This
        property is maintained by the classes that inherit this class
        :return: bool
        """
        return self.changed

    def get_note(self) -> str:
        """
        Returns the text of the note associated with the datapoint
        :return: String note
        """
        return self.note_var.get()

    def edit_note(self, event):
        """
        Presents a dialog that allows the note to be edited if it is not displayed on the widget
        :param event:
        :return: None
        """
        self.note_tip.hidetip()
        note = tksimpedialog.askstring(title="Edit Note", prompt="Note:", initialvalue=self.get_note())
        if note is not None:
            self.note_var.set(note)
            self.check_changed_note()
            self.note_tip = NoteTip(self, note)
            self.bind('<Button-3>', self.edit_note)


class BodyWeightWidget(MetricWidget):
    """
    A widget that allows the entry of a Body Weight datapoint
    """
    def __init__(self, parent, track_cfg: dp.TrackingConfig, datapoint: dp.BodyWeightDP = None,
                 show_note_field: bool = True):
        """
        Creates a gui.widgets.BodyWeightWidget
        :param parent: the GUI parent for this widget
        :param track_cfg: an instance of model.datapoints.TrackingConfig to provide the u.o.m associated with the
        datapoint.
        """
        MetricWidget.__init__(self, parent, track_cfg, datapoint, show_note_field)
        self.weight_var = ttkb.DoubleVar()
        self.weight_entry = DecimalMetricWidget(parent=self, dp_type=self.dp_type, uom=self.uom,
                                                min_label_width=METRIC_WIDGET_MIN_LBL_WIDTH, entry_width=6,
                                                label_grid_args={'column': 0, 'row': 0, 'padx': 5, 'pady': 5},
                                                entry_grid_args={'column': 1, 'row': 0, 'padx': 5, 'pady': 5})

        if datapoint:
            self.weight_entry.set_value(datapoint.data.value)
        self.weight_entry.bind('<FocusOut>', self.check_change)
        self.weight_entry.bind('<KeyPress>', self.handle_keypress)
        self.create_note_entry()
        self.grid(padx=5, pady=5)
        if not show_note_field:
            for child in self.children.values():
                child.bind('<Button-3>', self.edit_note)

    def get_value(self) -> Decimal:
        """
        Returns the body weight value
        :return: Decimal body weight value
        """
        return self.weight_entry.get_value()

    def zero_value(self) -> None:
        """
        Zeros the body weight value and set the note to an empty string
        :return: None
        """
        self.weight_entry.set_value(Decimal(0))
        self.note_entry.set_value('')

    def focus_set(self) -> None:
        """
        Sets the value widget in focus when the focus_set method on the widget is called
        :return: None
        """
        self.weight_entry.focus_set()

    def check_change(self, event):
        """
        Check to see if the widgets metric value has been changed
        :param event:
        :return: None
        """
        if self.datapoint and abs(self.datapoint.data.value - self.get_value()) > 0.1:
            self.changed = True


class BodyTempWidget(MetricWidget):
    """
    A widget that allows the entry of a Body Temperature datapoint
    """
    def __init__(self, parent, track_cfg: dp.TrackingConfig, datapoint: dp.BodyTemperatureDP=None,
                 show_note_field: bool = True):
        """
        Creates a gui.widgets.BodyTempWidget
        :param parent: the GUI parent for this widget
        :param track_cfg: an instance of model.datapoints.TrackingConfig to provide the u.o.m associated with the
        datapoint.
        :param datapoint: if not None, provides initial values for the metric
        :param show_note_field: boolean, controls whether the Note entry is shown in the frame or displayed by
        a right click
        """
        MetricWidget.__init__(self, parent, track_cfg, datapoint, show_note_field)
        self.temp_entry = DecimalMetricWidget(parent=self, dp_type=self.dp_type, uom=self.uom,
                                              min_label_width=METRIC_WIDGET_MIN_LBL_WIDTH, entry_width=6,
                                              label_grid_args={'column': 0, 'row': 0, 'padx': 5, 'pady': 5},
                                              entry_grid_args={'column': 1, 'row': 0, 'padx': 5, 'pady': 5})
        self.temp_entry.bind('<KeyPress>', self.handle_keypress)
        self.temp_entry.bind('<FocusOut>', self.check_change)
        if datapoint is not None:
            self.temp_entry.set_value(datapoint.data.value)
        self.grid(padx=5, pady=5)
        self.create_note_entry()
        if not show_note_field:
            for child in self.children.values():
                child.bind('<Button-3>', self.edit_note)

    def get_value(self) -> Decimal:
        """
        Returns the body temperature value
        :return: Decimal body temperature
        """
        return self.temp_entry.get_value()

    def zero_value(self) -> None:
        """
        Zeros the body temperature value and set the note to an empty string
        :return: None
        """
        self.temp_entry.set_value(Decimal(0))
        self.note_entry.set_value('')

    def focus_set(self) -> None:
        """
        Sets the focus on the temp_entry field when the focus_set methold on the widget is called
        :return: None
        """
        self.temp_entry.focus_set()

    def check_change(self, event):
        """
        Check to see if the widgets metric value has been changed
        :param event:
        :return: None
        """
        if self.datapoint and (super().is_changed() or abs(self.datapoint.data.value - self.get_value()) > 0.001):
            self.changed = True


class BloodPressureWidget(MetricWidget):
    """
    A widget that allows the entry of a Blood Pressure datapoint
    """
    def __init__(self, parent, track_cfg: dp.TrackingConfig, datapoint: dp.BloodPressureDP = None,
                 show_note_field: bool = True):
        """
        Creates a gui.widgets.BloodPressureWidget
        :param parent: the GUI parent for this widget
        :param track_cfg: an instance of model.datapoints.TrackingConfig to provide the u.o.m associated with the
        datapoint.
        :param datapoint: if not None, provides initial values for the metric
        :param show_note_field: boolean, controls whether the Note entry is shown in the frame or displayed by
        a right click
       """
        MetricWidget.__init__(self, parent, track_cfg, datapoint, show_note_field)
        self.systolic_entry = LabeledIntegerWidget(self, label_text='Systolic:',
                                                   label_width=self.label_width(dp.dptype_dp_map[dp.DataPointType.BP]
                                                                                .label(),  METRIC_WIDGET_MIN_LBL_WIDTH),
                                                   label_grid_args={'column': 0, 'row': 0, 'padx': 5, 'pady': 0},
                                                   entry_width=6,
                                                   entry_grid_args={'column': 1, 'row': 0, 'padx': 5, 'pady': 0})
        self.systolic_entry.bind('<KeyPress>', self.handle_keypress)
        self.systolic_entry.bind('<FocusOut>', self.check_change)
        bp_label = ttkb.Label(self, text='Blood Pressure:', width=self.label_width('Blood Pressure:',
                                                                                   METRIC_WIDGET_MIN_LBL_WIDTH),
                              anchor=tk.E)
        bp_label.grid(column=0, row=1, padx=5, pady=0)
        ttkb.Label(self, text=f'===========', anchor=tk.E).grid(column=1, row=1)
        ttkb.Label(self, text=f'{track_cfg.default_uom.abbreviation()}').grid(column=2, row=1, padx=5, pady=0)
        self.diastolic_entry = LabeledIntegerWidget(self, label_text='Diastolic:',
                                                    label_width=self.label_width(dp.dptype_dp_map[dp.DataPointType.BP]
                                                                                 .label(),  METRIC_WIDGET_MIN_LBL_WIDTH),
                                                    label_grid_args={'column': 0, 'row': 2, 'padx': 5, 'pady': 0},
                                                    entry_width=6,
                                                    entry_grid_args={'column': 1, 'row': 2, 'padx': 5, 'pady': 0})
        self.diastolic_entry.bind('<KeyPress>', self.handle_diastolic_keypress)
        self.diastolic_entry.bind('<FocusOut>', self.check_change)
        if datapoint is not None:
            self.systolic_entry.set_value(datapoint.data.systolic)
            self.diastolic_entry.set_value(datapoint.data.diastolic)

        self.create_note_entry()
        if not show_note_field:
            for child in self.children.values():
                child.bind('<Button-3>', self.edit_note)

    @staticmethod
    def label_width(text: str, min_width: int) -> int:
        """
        Calculates a width for the label based on the label text and a minumum width

        :param text: the label test
        :type text: str
        :param min_width: the minimum label width
        :type min_width: int
        :reFturn: the calculated label width
        :rtype: int

        """
        text_width = len(text) + 2
        if text_width < min_width:
            return min_width
        else:
            return text_width

    def get_value(self) -> (int, int):
        """
        Returns the systolic and diastolic values entered to the widget
        :return: tuple(int, int) representing the systolic and diastolic values
        """
        return self.systolic_entry.get_value(), self.diastolic_entry.get_value()

    def zero_value(self) -> None:
        """
        Zeros the systolic and diastolic values and set the note to an empty string
        :return: None
        """
        self.systolic_entry.set_value(0)
        self.diastolic_entry.set_value(0)
        self.note_entry.set_value('')

    def focus_set(self) -> None:
        """
        Sets the focus on the systolic_entry field when the focus_set method of the widget is called
        :return: None
        """
        self.systolic_entry.focus_set()

    def check_change(self, event):
        """
        Check to see if the widgets metric value has been changed
        :param event:
        :return: None
        """
        systolic, diastolic = self.get_value()
        if self.datapoint and (super().is_changed() or self.datapoint.data.systolic != systolic or
                               self.datapoint.data.diastolic != diastolic):
            self.changed = True

    def handle_diastolic_keypress(self, event):
        match event.keysym:
            case 'Up':
                self.systolic_entry.focus_set()
            case 'KP_Up':
                self.systolic_entry.focus_set()
            case _:
                self.handle_keypress(event)


class BloodGlucoseWidget(MetricWidget):
    """
    A widget that allows the entry of a Blood Glucose datapoint
    """
    def __init__(self, parent, track_cfg: dp.TrackingConfig, datapoint: dp.BloodGlucoseDP = None,
                 show_note_field: bool = True):
        """
        Creates a gui.widgets.BloodGlucoseWidget
        :param parent: the GUI parent for this widget
        :param track_cfg: an instance of model.datapoints.TrackingConfig to provide the u.o.m associated with the
        datapoint.
        :param datapoint: if not None, provides initial values for the metric
        :param show_note_field: boolean, controls whether the Note entry is shown in the frame or displayed by a right click

        """
        MetricWidget.__init__(self, parent, track_cfg, datapoint, show_note_field)
        self.bg_entry = IntegerMetricWidget(self, dp_type=track_cfg.dp_type, uom=track_cfg.default_uom,
                                            min_label_width=METRIC_WIDGET_MIN_LBL_WIDTH, entry_width=6,
                                            label_grid_args={'column': 0, 'row': 0, 'padx': 5, 'pady': 5},
                                            entry_grid_args={'column': 1, 'row': 0, 'padx': 5,
                                                             'pady': 5})
        self.bg_entry.bind('<KeyPress>', self.handle_keypress)
        self.bg_entry.bind('<FocusOut>', self.check_change)
        if datapoint is not None:
            self.bg_entry.set_value(datapoint.data.value)
        self.grid(padx=5, pady=5)
        self.create_note_entry()
        if not show_note_field:
            for child in self.children.values():
                child.bind('<Button-3>', self.edit_note)

    def get_value(self) -> int:
        """
        Returns the blood glucose value

        :return: int blood glucose value
        :rtype: int

        """
        return self.bg_entry.get_value()

    def zero_value(self) -> None:
        """
        Zeros the blood glucose value and set the note to an empty string

        :return: None

        """
        self.bg_entry.set_value(0)
        self.note_entry.set_value('')

    def focus_set(self) -> None:
        """
        Sets the focus on the bg_entry field when the focus_set method on the widget is called

        :return: None

        """
        self.bg_entry.focus_set()

    def check_change(self, event):
        """
        Check to see if the widgets metric value has been changed

        :param event:
        :type event: tkinter.Event
        :return: None

        """
        if self.datapoint and (super().is_changed() or self.datapoint.data.value != self.get_value()):
            self.changed = True


class PulseWidget(MetricWidget):
    """
    A widget the allows entry of a pulse metric
    """
    def __init__(self, parent, track_cfg: dp.TrackingConfig, datapoint: dp.PulseDP = None,
                 show_note_field: bool = True):
        """
        Creates a gui.widgets.PulseWidget
        :param parent: the GUI parent for this widget
        :param track_cfg: an instance of model.datapoints.TrackingConfig to provide the u.o.m associated with the
        datapoint.
        :param datapoint: if not None, provides initial values for the metric
        :param show_note_field: boolean, controls whether the Note entry is shown in the frame or displayed by a right click

        """
        MetricWidget.__init__(self, parent, track_cfg, datapoint, show_note_field)
        self.pulse_entry = IntegerMetricWidget(self, dp_type=track_cfg.dp_type, uom=track_cfg.default_uom,
                                               min_label_width=METRIC_WIDGET_MIN_LBL_WIDTH, entry_width=6,
                                               label_grid_args={'column': 0, 'row': 0, 'padx': 5, 'pady': 5},
                                               entry_grid_args={'column': 1, 'row': 0, 'padx': 5, 'pady': 5})
        self.grid(padx=5, pady=5)
        self.pulse_entry.bind('<FocusOut>', self.check_change)
        self.pulse_entry.bind('<KeyPress>', self.handle_keypress)
        if datapoint is not None:
            self.pulse_entry.set_value(datapoint.data.value)
        self.create_note_entry()
        if not show_note_field:
            for child in self.children.values():
                child.bind('<Button-3>', self.edit_note)

    def get_value(self) -> int:
        """
        Returns the value entered for the metric

        :return: int pulse metric value

        """
        return self.pulse_entry.get_value()

    def zero_value(self) -> None:
        """
        Zeros the pulse value and set the note to an empty string

        :return: None

        """
        self.pulse_entry.set_value(0)
        self.note_entry.set_value('')

    def focus_set(self) -> None:
        """
        Sets the focus on the pulse_entry field when the focus_set method of the widget is called

        :return: None

        """
        self.pulse_entry.focus_set()

    def check_change(self, event):
        """
        Check to see if the widgets metric value has been changed

        :param event:
        :type event: tkinter.Event
        :return: None

        """
        if self.datapoint and (super().is_changed() or self.datapoint.data.value != self.get_value()):
            self.changed = True


widget_union = Union[BloodPressureWidget, PulseWidget, BloodGlucoseWidget, BodyWeightWidget, BodyTempWidget]


class MetricWidgetFactory:
    """
    This factory class creates the apporpriate widget for the specified datapoint type, optionally setting
    the initial field values from an instance of biometrics_tracker.model.datapoints.DataPoint

    """
    @classmethod
    def make_widget(cls, parent, track_cfg: dp.TrackingConfig, datapoint: Optional[dp.DataPoint] = None,
                    show_note_field: bool = True) -> widget_union:
        """

        :param parent: parent GUI element for the new widget instance
        :param track_cfg: contains the DataPointType and UOM info to be used in creating the widget
        :type track_cfg: biometrics_tracker.model.datapoints.TrackingConfig
        :param datapoint: an optional datapoint to fill the widget field values
        :type datapoint: Optional[biometrics_tracker.model.datapoints.DataPoint]
        :param show_note_field: controls whether the widget Note field is shown on the widget of implemented
         as a right-click function
        :type show_note_field: bool
        :return: a metric widget appropriate to the specified datapoint type
        :rtype: biometrics_tracker.gui.widgets.widget_union

        """
        widget: Optional[widget_union] = None

        match track_cfg.dp_type:
            case dp.DataPointType.BODY_WGT:
                widget: MetricWidget = BodyWeightWidget(parent, track_cfg, datapoint,
                                                        show_note_field=show_note_field)
            case dp.DataPointType.BODY_TEMP:
                widget = BodyTempWidget(parent, track_cfg, datapoint, show_note_field=show_note_field)
            case dp.DataPointType.BP:
                widget = BloodPressureWidget(parent, track_cfg, datapoint, show_note_field=show_note_field)
            case dp.DataPointType.BG:
                widget = BloodGlucoseWidget(parent, track_cfg, datapoint, show_note_field=show_note_field)
            case dp.DataPointType.PULSE:
                widget = PulseWidget(parent, track_cfg, datapoint, show_note_field=show_note_field)
            case _:
                ...
        return widget


class DataPointWidget(ttkb.Frame):
    """
    A frame that contains DataPoint Taken date and time and a MetricWidget subclass to allow editing of DataPoint info

    """
    def __init__(self, parent, track_cfg: dp.TrackingConfig, datapoint: dp.DataPoint):
        """
        Create an instance of DataPointWidget

        :param parent: the GUI parent of this Frame
        :param track_cfg: a TrackingConfig object associated with a Person.  Carries the DataPointType and default UOM.
        :type track_cfg: biometrics_tracker.model.datapoints.TrackingConfig
        :param datapoint: the DataPoint to be edited
        :type datapoint: biometrics_tracker.model.datapoints.DataPoint
        :returns: None

        """
        ttkb.Frame.__init__(self, parent)
        self.track_cfg = track_cfg
        self.datapoint = datapoint
        ttkb.Label(self, text='Recorded:', width=METRIC_WIDGET_MIN_LBL_WIDTH, anchor=tk.E).grid(column=0, row=0,
                                                                                                padx=5, pady=5)
        self.taken_date_widget = DateWidget(self, default_value=datapoint.taken.date())
        self.taken_date_widget.grid(column=1, row=0, padx=0, pady=5, sticky=tk.W)
        ttkb.Label(self, text='at', width=3).grid(column=2, row=0, sticky=tk.NW, padx=2, pady=5)
        self.taken_time_widget = TimeWidget(self, default_value=datapoint.taken.time())
        self.taken_time_widget.grid(column=4, row=0, sticky=tk.W, padx=0, pady=5)
        self.metric_widget = MetricWidgetFactory.make_widget(self, track_cfg, datapoint)
        self.metric_widget.grid(column=0, row=1, columnspan=5)
        self.taken_date_widget.prev_entry = parent
        self.taken_date_widget.next_entry = self.taken_time_widget
        self.taken_time_widget.prev_entry = self.taken_date_widget
        self.taken_time_widget.next_entry = self.metric_widget

    def focus_set(self) -> None:
        """
        Delete focus_set calls to the Taken date

        :return: None

        """
        self.taken_date_widget.focus_set()

    def get_taken(self) -> datetime:
        """
        Returns a datetime created from the values fo the Taken date and time widgets

        :returns: the Taken datetime
        :rtype: datetime.datetime

        """
        return utilities.mk_datetime(self.taken_date_widget.get_date(), self.taken_time_widget.get_time())

    def is_changed(self) -> bool:
        """
        Have the edit prompt values been changed

        :return: True if there have been changes
        :rtype: bool

        """
        new_taken: datetime = utilities.mk_datetime(self.taken_date_widget.get_date(),
                                                    self.taken_time_widget.get_time())
        return (not utilities.compare_mdyhms(new_taken, self.datapoint.taken)) or \
            self.metric_widget.is_changed()

    def get_datapoint(self) -> dp.DataPoint:
        """
        Return a DataPoint objects whose values are taken from the prompt values

        :return: a Datapoint
        :rtype: biometrics_tracker.model.datapoint.DataPoint

        """
        if not self.is_changed:
            return self.datapoint
        else:
            self.datapoint.taken = utilities.mk_datetime(self.taken_date_widget.get_date(),
                                                         self.taken_time_widget.get_time())
            if self.metric_widget.is_changed:
                if self.datapoint.type == dp.DataPointType.BP:
                    self.datapoint.data.systolic, self.datapoint.data.diastolic = self.metric_widget.get_value()
                else:
                    self.datapoint.data.value = self.metric_widget.get_value()
                self.datapoint.note = self.metric_widget.get_note()
            return self.datapoint

    def get_metric_widget(self) -> Union[BloodPressureWidget, PulseWidget, BloodGlucoseWidget, BodyWeightWidget,
                                         BodyTempWidget]:
        """
        Returns the MetricWidget subclass contained in the Frame

        :return: a MetricWidget subclass
        :rtype: Union[BloodPressureWidget, PulseWidget, BloodGlucoseWidget, BodyWeightWidget, BodyTempWidget]

        """
        return self.metric_widget


class ImportExportFieldWidget(ttkb.Frame):
    """
    A widget that represents a field in an import or export file.  Each widget instance is associated with one
    of the string values defined at the beginning of the model.importers module
    """
    def __init__(self, parent, index: int, field_name: str, field_name_values: list[str]):
        ttkb.Frame.__init__(self, parent)
        self.index = index + 1
        ttkb.Label(self, text=f'{self.index}.').grid(column=0, row=0, sticky=NSEW, padx=2, pady=2)
        self.field_name_var = ttkb.StringVar()
        self.field_name_var.set(imp.IGNORE)
        self.field_name_entry = ttk.Combobox(self, textvariable=self.field_name_var, values=field_name_values)
        self.field_name_entry.grid(column=1, row=0, sticky=NSEW, padx=2, pady=2)
        self.grid(padx=3, pady=3)

    def is_changed(self) -> bool:
        """
        Has a field name selection changed
        :return: boolean True is field name has changed, False otherwise
        """
        return self.field_name != self.field_name_var.get()

    def field_index(self) -> int:
        """
        Returns the int value that specifies the position of the field in the CSV file
        :return: int field index
        """
        return self.index

    def field_name(self) -> str:
        """
        Returns the selected field anme
        :return: string field name
        """
        return self.field_name_var.get()


class DaysOfMonthWidget(ttkb.Frame):
    """
    Presents a calendar frame for the specified month/year, with a checkbox for each day of the month.  Allows the
    user to select days of the month.  Optionally the calendar can naviate through the months via 'previous month'
    and 'next month' buttons
    """
    def __init__(self, parent, month: int, year: int, prev_month_handler: Optional[Callable] = None,
                 next_month_handler: Optional[Callable] = None):
        """
        Create an instance of DaysOfMonthWidget

        :param parent: the GUI parent for this frame
        :param month: the month to be presented
        :type month: int
        :param year: the year to be presented
        type year: int
        :param prev_month_handler:  a callable to handle clicks on the 'previous month button'.  If None is provided,
        no 'previous month' button will be created.
        :type prev_month_handler: Callable
        :param next_month_handler: a callable to handle clicks on the 'next month' button.  If None is provided,
        no 'next month' button will be created.

        """
        ttkb.Frame.__init__(self, parent)
        self.month: int = month
        self.year: int = year
        col: int = 0
        if prev_month_handler is not None:
            ttkb.Button(self, text='<', command=prev_month_handler).grid(column=0, row=0)
        month_start = date(year, month, 1)
        ttkb.Label(self, text=month_start.strftime('%B %Y')).grid(column=1, row=0, columnspan=2)
        if next_month_handler is not None:
            ttkb.Button(self, text='>', command=next_month_handler).grid(column=7, row=0)
        for day in dp.WeekDay:
            ttkb.Label(self, text=day.name[0:3], width=4).grid(column=col, row=1, padx=1, pady=1)
            col += 1
        self.month_start_dow = month_start.weekday()
        self.day_grid: dict[int: (Checkbutton, ttkb.IntVar)] = {}
        self.row_col_map: dict[tuple[int, int]: int] = {}
        self.max_dom = utilities.max_dom(self.month)
        dom: int = 1
        for row in range(2, 8):
            cols = range(0, 7)
            if row == 2:
                cols = range(self.month_start_dow, 7)
            for col in cols:
                if dom <= self.max_dom:
                    dom_var = ttkb.IntVar()
                    dom_var.set(0)
                    dom_check = Checkbutton(self, text=f'{dom:2d}', variable=dom_var, padding=1, width=4)
                    dom_check.grid(column=col, row=row)
                    dom_check.bind('<KeyPress>', self.handle_arrows)
                    dom_check.row = row
                    dom_check.column = col
                    self.day_grid[dom] = (dom_check, dom_var)
                    self.row_col_map[(row, col)] = dom
                    dom += 1
        self.grid()

    def focus_set(self):
        """
        Delegates invocation of the focus_set method to the check box for the first day of the month.

        :return: None

        """
        self.day_grid[1][0].focus_set()

    def disable(self):
        """
        Disables each of the days of the month check boxes

        :return: None

        """
        for tup in self.day_grid.values():
            tup[0].configure(state=DISABLED)

    def enable(self):
        """
        Enables each of the days of the month check boxes

        :return: None

        """
        for tup in self.day_grid.values():
            tup[0].configure(state=NORMAL)

    def handle_arrows(self, event):
        """
        Handles keypresses of the arrow keys to allow navigation within the days of the month.

        :param event:
        :return: None

        """
        row: int = event.widget.row
        col: int = event.widget.column
        dom: int = self.row_col_map[(row, col)]
        match event.keysym:
            case 'Up':
                if dom - 7 > 0:
                    self.day_grid[dom - 7][0].focus_set()
            case 'KP_Up':
                if dom - 7 > 0:
                    self.day_grid[dom - 7][0].focus_set()
            case 'Down':
                if dom + 7 <= self.max_dom:
                    self.day_grid[dom + 7][0].focus_set()
            case 'KP_Down':
                if dom + 7 < self.max_dom:
                    self.day_grid[dom + 7][0].focus_set()
            case 'Left':
                if dom > 1:
                    self.day_grid[dom - 1][0].focus_set()
            case 'KP_Left':
                if dom > 1:
                    self.day_grid[dom - 1][0].focus_set()
            case 'Right':
                if dom < self.max_dom:
                    self.day_grid[dom + 1][0].focus_set()
            case 'Right':
                if dom < self.max_dom:
                    self.day_grid[dom + 1][0].focus_set()
            case _:
                pass

    def get_days_of_month(self) -> list[int]:
        """
        Returns a list of the days of the month selected by the user

        :return: list[int]
        """
        days: list[int] = []
        for dom, dom_pair in self.day_grid.items():
            if dom_pair[1].get() > 0:
                days.append(dom)
        return days

    def set_days_of_month(self, days: list[int]):
        """
        Sets the day of month check boxes for a list of days of the month

        :param days: a list of the days to be set
        :type days: list[int]
        :return: None

        """
        for dom in days:
            self.day_grid[dom][1].set(1)


DaysOfMonthsType = dict[(int, int): list[int]]


class DaysOfMonthManager:
    """
    Manages a DaysOfMonthWidget by handling navigation (if it is specified) and collecting the days checked for
    each month.
    """
    def __init__(self, parent, month: int, year: int, column: int, row: int, columnspan: int = 1,
                 allow_navigation: bool = False):
        """
        Create an instance of DaysOfMonthManager widget

        :param parent: GUI parent of the widget
        :param month: starting month for the widget
        :type month: int
        :param year: starting year for the widget
        :type year: int
        :param column: column the widget is to be gridded to
        :type column: int
        :param row: row the widget is to be gridded to
        :type row: int
        :param columnspan: columnspan the widget is to be gridded to
        :type columnspan: int
        :param allow_navigation: allow navigation from month to month
        :type allow_navigation: bool

        """
        self.parent = parent
        self.month: int = month
        self.year: int = year
        self.column: int = column
        self.row: int = row
        self.columnspan: int = columnspan
        self.allow_navigation = allow_navigation
        self.days_of_months: DaysOfMonthsType = {}
        if allow_navigation:
            self.widget = DaysOfMonthWidget(parent=parent, month=month, year=year, prev_month_handler=self.prev_month,
                                            next_month_handler=self.next_month)
        else:
            self.widget = DaysOfMonthWidget(parent, month, year)
        self.widget.grid(column=column, row=row, columnspan=columnspan)

    def focus_set(self):
        """
        Pass focus_set calls through to the widget
        :return:
        """
        self.widget.focus_set()

    def disable(self):
        """
        Delegate disable to the widget

        :return: None

        """
        self.widget.disable()

    def enable(self):
        """
        Delegate enable to the widget

        :return: None

        """
        self.widget.enable()

    def prev_month(self):
        """
        Previous month navigation handler.  Prior to navigation, the checkboxes for the current month are collect
        and placed in a dictionary keyed by year and month. After the new widget is created, the values collected
        (if any) when the month was navigated away from are used to preset the checkboxes

        :return: None
        """
        self.days_of_months[(self.year, self.month)] = self.widget.get_days_of_month()
        if self.month > 1:
            self.month -= 1
        else:
            self.month = 12
            self.year -= 1
        self.widget.forget()
        self.widget = DaysOfMonthWidget(self.parent, self.month, self.year, self.prev_month, self.next_month)
        self.widget.grid(column=self.column, row=self.row, columnspan=self.columnspan)
        if (self.year, self.month) in self.days_of_months:
            self.widget.set_days_of_month(self.days_of_months[(self.year, self.month)])

    def next_month(self):
        """
         Next month navigation handler.  Prior to navigation, the checkboxes for the current month are collect
         and placed in a dictionary keyed by year and month. After the new widget is created, the values collected
        (if any) when the month was navigated away from are used to preset the checkboxes

         :return: None

         """
        self.days_of_months[(self.year, self.month)] = self.widget.get_days_of_month()
        if self.month < 12:
            self.month += 1
        else:
            self.month = 1
            self.year += 1
        self.widget.forget()
        self.widget = DaysOfMonthWidget(self.parent, self.month, self.year, self.prev_month, self.next_month)
        self.widget.grid(column=self.column, row=self.row, columnspan=self.columnspan)
        if (self.year, self.month) in self.days_of_months:
            self.widget.set_days_of_month(self.days_of_months[(self.year, self.month)])

    def get_days_of_months(self) -> DaysOfMonthsType:
        """
        Returns the dict containing the days of the months that were checked.  Before returning, the entries for
        months in which no boxes where checked are removed from the dict.

        :return: DaysOfMonthsType (dict[[int, int]: list[int]]
        """
        self.days_of_months[(self.year, self.month)] = self.widget.get_days_of_month()
        delete_list: list[(int, int)] = []
        for ym, days in self.days_of_months.items():
            if len(days) == 0:
                delete_list.append(ym)
        for ym in delete_list:
            del self.days_of_months[ym]
        return self.days_of_months

    def get_days_of_month(self) -> list[int]:
        return self.widget.get_days_of_month()

    def set_days_of_months(self, doms: DaysOfMonthsType):
        """
        Set the initial checked state

        :param doms: the map of the days of months that will be checked
        :type doms: biometrics_tracker.gui.widgets.DaysOfMonthsType
        :return: None

        """
        self.days_of_months = doms
        self.widget.set_days_of_month(self.days_of_months[(self.year, self.month)])


class PlaceholderWidget(ttkb.Frame):
    def __init__(self, parent, rows: int, width: int):
        ttkb.Frame.__init__(self, parent)
        for row in range(0, rows):
            ttkb.Label(self, text="              ", width=width).grid(column=0, row=row)

    def is_changed(self) -> bool:
        """
        This method assures that the PlaceholderWidget presents an interface that is consistant with the
        DataPointWidget

        :returns: False
        :rtype: bool

        """
        return False


class DataPointTypeComboWidget(ttkb.Frame):
    """
    A ComboBox for selecting an option from a list of DataPointType enum values
    """
    def __init__(self, parent, person: Optional[dp.Person]):
        """
        Creates an instance of DataPointTypeComboWidget

        :param parent: The GUI parent of this widget:
        :param person: a Person instance associated with the currently selected person.  The TrackingConfig instances are used to limit the DataPointTypes listed in the combo box
        :type person: biometrics_tracker.model.datapoints.Person
        """
        ttkb.Frame.__init__(self, parent)
        self.dp_type_var = ttkb.StringVar()
        combobox_list: list[str] = []
        self.combobox_map: dict[str, dp.DataPointType] = {}
        for dp_type, datapoint in dp.dptype_dp_map.items():
            if person is None or person.is_tracked(dp_type):
                combobox_list.append(datapoint.label())
                self.combobox_map[datapoint.label()] = dp_type
        self.dp_type_entry = ttkb.Combobox(self, textvariable=self.dp_type_var,
                                           values=combobox_list)
        self.dp_type_entry.bind('<<ComboboxSelected>>', self.retrieve_selection)
        self.dp_type_entry.grid(column=0, row=0)
        self.dp_type: Optional[dp.DataPointType] = None

    def retrieve_selection(self, event):
        """
        Retrieve the selection and convert it to a DataPointType.  The values used for the list are the
        returned value from the label method of the corresponding container class.  For instance, the
        value for DataPointType.BG selection comes from BloodGlucose.label()

        :param event:
        :return: None

        """
        dp_label: str = self.dp_type_var.get()
        self.dp_type = self.combobox_map[dp_label]

    def set_selection(self, dp_type: dp.DataPointType):
        """
        Set the selected item in the list of DataPointType enums.  This method performs the inverse of the
        operation performed by the retrieve_selection method

        :param dp_type: The DataPointType enum value to be set
        :type dp_type: biometrics_tracker.model.datapoints.DataPointType
        :return: None

        """
        self.dp_type_var.set(dp.dptype_dp_map[dp_type].label())
        self.dp_type = dp_type

    def get_dp_type(self) -> dp.DataPointType:
        """
        Returns the DataPointType selected by the user

        :return:  biometrics_tracker.model.datapoints.DataPointType

        """
        return self.dp_type


class ScheduleWidget(ttkb.Frame):
    """
    A base class for widgets corresponding to the various schedule types; Monthly, Weekly, etc.

    """
    def __init__(self, parent, person_id: str, seq_nbr: int, dp_type: dp.DataPointType, starts_on: date, ends_on: date,
                 note: str, interval: int, when_time: Optional[time], suspended: bool,
                 last_triggered: Optional[datetime]):
        """
        Creates an instance of ScheduleWidget - invoked by it subclasses

        :param parent: the GUI parent of the widget
        :param person_id: the id of the person associated with the ScheduleEntry
        :type person_id: str
        :param seq_nbr: the sequence number associated with the ScheduleEntry
        :type person_id: int
        :param dp_type: the DataPointType associated with the ScheduleEntry
        :type dp_type:   biometrics_tracker.model.datapoints.DataPointType
        :param starts_on: the starting date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param ends_on:  the ending date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param note: the note associated with the ScheduleEntry
        :type note: str
        :param interval: the interval (hours, days, weeks, etc) associated with the ScheduleEntry
        :type interval: int
        :param when_time: the trigger time associated with the ScheduleEntry
        :type when_time: datetime.time
        :param suspended: has the ScheduleEntry been suspended
        :type suspended: bool
        :param last_triggered: the last date/time on which the ScheduleEntry was triggered
        :type last_triggered: datetime.datetime

        """
        ttkb.Frame.__init__(self, parent)
        self.parent = parent
        self.person_id: str = person_id
        self.seq_nbr = seq_nbr
        self.dp_type: dp.DataPointType = dp_type
        self.starts_on: date = starts_on
        self.ends_on: date = ends_on
        self.note: str = note
        self.suspended = suspended
        self.last_triggered: datetime = last_triggered
        self.interval_var = ttkb.IntVar()
        self.interval_var.set(interval)
        self.interval_entry = ttkb.Entry(self, textvariable=self.interval_var)
        if when_time is not None:
            self.when_time_entry = TimeWidget(self, default_value=when_time)
        else:
            self.when_time_entry = TimeWidget(self)

    def enable(self):
        """
        Enable the interval and when time widgets for entry

        :return: None

        """
        self.interval_entry.configure(state=NORMAL)
        self.when_time_entry.enable()

    def disable(self):
        """
        Disable the interval and when time widgets for entry

        :return: None

        """
        self.interval_entry.configure(state=DISABLED)
        self.when_time_entry.disable()

    def focus_set(self):
        """
        Delegate focus_set to the interval widget

        :return: None
        """
        self.interval_entry.focus_set()

    @abstractmethod
    def get_schedule_entry(self) -> dp.ScheduleEntry:
        """
        Build a ScheduleEntry instance from the widget values and return it

        :return: the created ScheduleEntry
        :rtype: biometrics_tracker.model.datapoints.ScheduleEntry

        """
        pass


class ScheduleDialog(tk.Toplevel):
    """
    Implements a dialog version of on of the ScheduleEntry widgets.  Not used in the current codebase
    """
    def __init__(self, widget: ScheduleWidget, cancel_action: Callable, save_action: Callable):
        tk.Toplevel.__init__(self)
        self.widget = widget
        self.widget.grid(column=0, row=0, columnspan=3)
        ttkb.Button(self, text='Cancel', command=cancel_action).grid(column=0, row=1)
        ttkb.Button(self, text='Save', command=save_action).grid(column=2, row=1)


class HourlyScheduleWidget(ScheduleWidget):
    """
    A widget to present and modify the info related to ScheduleEntry instances with an Hourly frequency

    """
    def __init__(self, parent, person_id: str, seq_nbr: int, dp_type: dp.DataPointType, starts_on: date, ends_on: date,
                 note: str, interval: int, when_time: Optional[time], suspended: bool,
                 last_triggered: Optional[datetime]):
        """
        Creates an instance of HourlyScheduleWidget

        :param parent: the GUI parent of the widget
        :param person_id: the id of the person associated with the ScheduleEntry
        :type person_id: str
        :param seq_nbr: the sequence number associated with the ScheduleEntry
        :type person_id: int
        :param dp_type: the DataPointType associated with the ScheduleEntry
        :type dp_type:   biometrics_tracker.model.datapoints.DataPointType
        :param starts_on: the starting date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param ends_on:  the ending date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param note: the note associated with the ScheduleEntry
        :type note: str
        :param interval: the interval (hours, days, weeks, etc) associated with the ScheduleEntry
        :type interval: int
        :param when_time: the trigger time associated with the ScheduleEntry
        :type when_time: datetime.time
        :param suspended: has the ScheduleEntry been suspended
        :type suspended: bool
        :param last_triggered: the last date/time on which the ScheduleEntry was triggered
        :type last_triggered: datetime.datetime

        """
        ScheduleWidget.__init__(self, parent, person_id, seq_nbr, dp_type, starts_on, ends_on, note, interval,
                                when_time, suspended, last_triggered)
        row: int = 0
        ttkb.Label(self, text='Hourly Schedule').grid(column=0, row=row, columnspan=4, padx=5, pady=5)
        row += 1
        ttkb.Label(self, text='Every:').grid(column=0, row=row, padx=5, pady=5)
        self.interval_entry.grid(column=1, row=row, sticky=tk.W)
        ttkb.Label(self, text='Hours').grid(column=2, row=row, padx=5, pady=5)
        row += 1
        ttkb.Label(self, text='First Time:').grid(column=0, row=row, padx=5, pady=5)
        self.when_time_entry.grid(column=1, row=row)
        self.when_time_entry.set_prev_entry(self.interval_entry)
        self.when_time_entry.set_next_entry(self.interval_entry)

    def get_schedule_entry(self) -> dp.ScheduleEntry:
        """
        Create and return an instance of ScheduleEntry

        :param self:

        :return: the created instance
        :rtype: biometrics_tracker.model.datapoints.ScheduleEntry

        """
        return dp.ScheduleEntry(person_id=self.person_id, seq_nbr=self.seq_nbr, frequency=dp.FrequencyType.Hourly,
                                dp_type=self.dp_type, note=self.note, weekdays=[], days_of_month=[],
                                interval=self.interval_var.get(), when_time=self.when_time_entry.get_time(),
                                starts_on=self.starts_on, ends_on=self.ends_on, suspended=self.suspended,
                                last_triggered=self.last_triggered)


class DailyScheduleWidget(ScheduleWidget):
    """
    A widget to present and modify the info related to ScheduleEntry instances with a Daily frequency

    """

    def __init__(self, parent, person_id: str, seq_nbr: int, dp_type: dp.DataPointType, starts_on: date, ends_on: date,
                 note: str, interval: int, when_time: Optional[time], suspended, last_triggered: Optional[datetime]):
        """
        Creates an instance of DailyScheduleWidget

        :param parent: the GUI parent of the widget
        :param person_id: the id of the person associated with the ScheduleEntry
        :type person_id: str
        :param seq_nbr: the sequence number associated with the ScheduleEntry
        :type person_id: int
        :param dp_type: the DataPointType associated with the ScheduleEntry
        :type dp_type:   biometrics_tracker.model.datapoints.DataPointType
        :param starts_on: the starting date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param ends_on:  the ending date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param note: the note associated with the ScheduleEntry
        :type note: str
        :param interval: the interval (hours, days, weeks, etc) associated with the ScheduleEntry
        :type interval: int
        :param when_time: the trigger time associated with the ScheduleEntry
        :type when_time: datetime.time
        :param suspended: has the ScheduleEntry been suspended
        :type suspended: bool
        :param last_triggered: the last date/time on which the ScheduleEntry was triggered
        :type last_triggered: datetime.datetime

        """
        ScheduleWidget.__init__(self, parent, person_id, seq_nbr, dp_type, starts_on, ends_on, note, interval,
                                when_time, suspended, last_triggered)
        row: int = 0
        ttkb.Label(self, text='Daily Schedule').grid(column=0, row=row, columnspan=4, padx=5, pady=5)
        row += 1
        ttkb.Label(self, text='Every').grid(column=0, row=row, padx=5, pady=5)
        self.interval_entry.grid(column=1, row=row)
        ttkb.Label(self, text='Days').grid(column=2, row=row, padx=5, pady=5)
        row += 1
        ttkb.Label(self, text='Time:').grid(column=0, row=row, padx=5, pady=5)
        self.when_time_entry.grid(column=1, row=row, padx=5, pady=5)
        self.when_time_entry.set_prev_entry(self.interval_entry)
        self.when_time_entry.set_next_entry(self.interval_entry)

    def get_schedule_entry(self) -> dp.ScheduleEntry:
        """
        Create and return an instance of ScheduleEntry

        :param self:
        :return: the created instance
        :rtype: biometrics_tracker.model.datapoints.ScheduleEntry

        """
        return dp.ScheduleEntry(person_id=self.person_id, seq_nbr=self.seq_nbr, frequency=dp.FrequencyType.Daily,
                                dp_type=self.dp_type, note=self.note, weekdays=[], days_of_month=[],
                                interval=self.interval_var.get(), when_time=self.when_time_entry.get_time(),
                                starts_on=self.starts_on, ends_on=self.ends_on, suspended=self.suspended,
                                last_triggered=self.last_triggered)


class WeeklyScheduleWidget(ScheduleWidget):
    """
    A widget to present and modify the info related to ScheduleEntry instances with a Weekly frequency

    """
    def __init__(self, parent, person_id: str, seq_nbr: int, dp_type: dp.DataPointType, starts_on: date, ends_on: date,
                 note: str, weekdays: list[dp.WeekDay], interval: int, when_time: Optional[time],
                 suspended, last_triggered: Optional[datetime]):
        """
        Creates an instance of WeeklyScheduleWidget

        :param parent: the GUI parent of the widget
        :param person_id: the id of the person associated with the ScheduleEntry
        :type person_id: str
        :param seq_nbr: the sequence number associated with the ScheduleEntry
        :type person_id: int
        :param dp_type: the DataPointType associated with the ScheduleEntry
        :type dp_type:   biometrics_tracker.model.datapoints.DataPointType
        :param starts_on: the starting date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param ends_on:  the ending date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param note: the note associated with the ScheduleEntry
        :type note: str
        :param interval: the interval (hours, days, weeks, etc) associated with the ScheduleEntry
        :type interval: int
        :param when_time: the trigger time associated with the ScheduleEntry
        :type when_time: datetime.time
        :param suspended: has the ScheduleEntry been suspended
        :type suspended: bool
        :param last_triggered: the last date/time on which the ScheduleEntry was triggered
        :type last_triggered: datetime.datetime

        """
        ScheduleWidget.__init__(self, parent, person_id, seq_nbr, dp_type, starts_on, ends_on, note, interval,
                                when_time, suspended, last_triggered)
        row: int = 0
        ttkb.Label(self, text='Weekly Schedule').grid(column=0, row=row, columnspan=4, padx=5, pady=5)
        row += 1
        ttkb.Label(self, text='Every').grid(column=0, row=row, padx=5, pady=5)
        self.interval_entry.grid(column=1, row=row)
        ttkb.Label(self, text='Weeks').grid(column=2, row=row, padx=5, pady=5)
        row += 1
        ttkb.Label(self, text='Time:').grid(column=0, row=row, padx=5, pady=5)
        self.when_time_entry.grid(column=1, row=row, padx=5, pady=5)
        row += 1
        self.weekday_entry = WeekDayCheckbuttonWidget(self)
        self.weekday_entry.set_checked_days(weekdays)
        self.weekday_entry.grid(column=0, row=row, columnspan=2, padx=5, pady=5)
        self.when_time_entry.set_prev_entry(self.interval_entry)
        self.when_time_entry.set_next_entry(self.weekday_entry)

    def get_schedule_entry(self) -> dp.ScheduleEntry:
        """
        Create and return an instance of ScheduleEntry

        :param self:

        :return: the created instance
        :rtype: biometrics_tracker.model.datapoints.ScheduleEntry

        """
        return dp.ScheduleEntry(person_id=self.person_id, seq_nbr=self.seq_nbr, frequency=dp.FrequencyType.Weekly,
                                dp_type=self.dp_type, note=self.note, weekdays=self.weekday_entry.get_checked_days(),
                                days_of_month=[], interval=self.interval_var.get(),
                                when_time=self.when_time_entry.get_time(), starts_on=self.starts_on,
                                ends_on=self.ends_on, suspended=self.suspended, last_triggered=self.last_triggered)


class MonthlyScheduleWidget(ScheduleWidget):
    """
    A widget to present and modify the info related to ScheduleEntry instances with a Monthly frequency

    """
    def __init__(self, parent, person_id: str, seq_nbr: int, dp_type: dp.DataPointType, starts_on: date, ends_on: date,
                 note: str, days_of_month: list[int], interval: int, when_time: Optional[time], suspended,
                 last_triggered: Optional[datetime]):
        """
        Creates an instance of WeeklyScheduleWidget

        :param parent: the GUI parent of the widget
        :param person_id: the id of the person associated with the ScheduleEntry
        :type person_id: str
        :param seq_nbr: the sequence number associated with the ScheduleEntry
        :type person_id: int
        :param dp_type: the DataPointType associated with the ScheduleEntry
        :type dp_type:   biometrics_tracker.model.datapoints.DataPointType
        :param starts_on: the starting date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param ends_on:  the ending date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param note: the note associated with the ScheduleEntry
        :type note: str
        :param interval: the interval (hours, days, weeks, etc) associated with the ScheduleEntry
        :type interval: int
        :param when_time: the trigger time associated with the ScheduleEntry
        :type when_time: datetime.time
        :param suspended: has the ScheduleEntry been suspended
        :type suspended: bool
        :param last_triggered: the last date/time on which the ScheduleEntry was triggered
        :type last_triggered: datetime.datetime

        """
        ScheduleWidget.__init__(self, parent, person_id, seq_nbr, dp_type, starts_on, ends_on, note, interval,
                                when_time, suspended, last_triggered)
        row: int = 0
        ttkb.Label(self, text='Monthly Schedule').grid(column=0, row=row, columnspan=4, padx=5, pady=5)
        row += 1
        ttkb.Label(self, text='Every').grid(column=0, row=row, padx=5, pady=5)
        self.interval_entry.grid(column=1, row=row)
        ttkb.Label(self, text='Months').grid(column=2, row=row, padx=5, pady=5)
        row += 1
        ttkb.Label(self, text='Time:').grid(column=0, row=row, padx=5, pady=5)
        self.when_time_entry.grid(column=1, row=row, padx=5, pady=5)
        row += 1
        self.dom_entry = DaysOfMonthManager(parent=self, month=self.starts_on.month, year=self.starts_on.year,
                                            column=0, row=row, columnspan=4)
        self.dom_entry.set_days_of_months({(self.starts_on.year, self.starts_on.month): days_of_month})
        self.when_time_entry.set_prev_entry(self.interval_entry)
        self.when_time_entry.set_next_entry(self.dom_entry)

    def get_schedule_entry(self) -> dp.ScheduleEntry:
        """
        Create and return an instance of ScheduleEntry

        :param self:

        :return: the created instance
        :rtype: biometrics_tracker.model.datapoints.ScheduleEntry

        """
        return dp.ScheduleEntry(person_id=self.person_id, seq_nbr=self.seq_nbr, frequency=dp.FrequencyType.Monthly,
                                dp_type=self.dp_type, note=self.note, weekdays=[],
                                days_of_month=self.dom_entry.get_days_of_month(),
                                interval=self.interval_var.get(), when_time=self.when_time_entry.get_time(),
                                starts_on=self.starts_on, ends_on=self.ends_on, suspended=self.suspended,
                                last_triggered=self.last_triggered)


class OneTimeScheduleWidget(ScheduleWidget):
    """
    A widget to present and modify the info related to ScheduleEntry instances with a Once frequency

    """
    def __init__(self, parent, person_id: str, seq_nbr: int, dp_type: dp.DataPointType, starts_on: date, ends_on: date,
                 note: str, interval: int, when_time: Optional[time], suspended: bool,
                 last_triggered: Optional[datetime]):
        """
        Creates an instance of WeeklyScheduleWidget

        :param parent: the GUI parent of the widget
        :param person_id: the id of the person associated with the ScheduleEntry
        :type person_id: str
        :param seq_nbr: the sequence number associated with the ScheduleEntry
        :type person_id: int
        :param dp_type: the DataPointType associated with the ScheduleEntry
        :type dp_type:   biometrics_tracker.model.datapoints.DataPointType
        :param starts_on: the starting date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param ends_on:  the ending date associated with the ScheduleEntry
        :type starts_on: datetime.date
        :param note: the note associated with the ScheduleEntry
        :type note: str
        :param interval: the interval (hours, days, weeks, etc) associated with the ScheduleEntry
        :type interval: int
        :param when_time: the trigger time associated with the ScheduleEntry
        :type when_time: datetime.time
        :param suspended: has the ScheduleEntry been suspended
        :type suspended: bool
        :param last_triggered: the last date/time on which the ScheduleEntry was triggered
        :type last_triggered: datetime.datetime

        """
        ScheduleWidget.__init__(self, parent, person_id, seq_nbr, dp_type, starts_on, ends_on, note, interval,
                                when_time, suspended, last_triggered)
        row: int = 0
        ttkb.Label(self, text='One Time Schedule').grid(column=0, row=row, columnspan=4, padx=5, pady=5)
        row += 1
        row += 1
        ttkb.Label(self, text='Time on Start Date:').grid(column=0, row=row, padx=5, pady=5)
        self.when_time_entry.grid(column=1, row=row, padx=5, pady=5)

    def get_schedule_entry(self) -> dp.ScheduleEntry:
        """
        Create and return an instance of ScheduleEntry

        :param self:

        :return: the created instance
        :rtype: biometrics_tracker.model.datapoints.ScheduleEntry

        """
        return dp.ScheduleEntry(person_id=self.person_id, seq_nbr=self.seq_nbr, frequency=dp.FrequencyType.One_Time,
                                dp_type=self.dp_type, note=self.note, weekdays=[], days_of_month=[],
                                interval=0, when_time=self.when_time_entry.get_time(),
                                starts_on=self.starts_on, ends_on=self.ends_on, suspended=self.suspended,
                                last_triggered=self.last_triggered)


class PayloadLabel(ttkb.Label):
    """
    A subclass of ttkbootstrap.Label that adds a payload property to carry on instance of some class ossocated
    with the text of the label.  This can be used to make the label a selection widget.
    """
    def __init__(self, parent, text, width, payload):
        ttkb.Label.__init__(self, master=parent, text=text, width=width)
        self.payload = payload


class PayloadCheckbutton(Checkbutton):
    """
    A subclass of biometrics_tracker.gui.widgets.Checkbutton that adds a payload property to carry an instance of
    some class associated with the Checkbutton.
    """
    def __init__(self, parent, text, variable, command=None, padding=None, width=None, payload: Any = None):
        Checkbutton.__init__(self, parent, text, variable, command, padding, width)
        self.payload = payload


class ErrorMessageDialog(dialogs.MessageDialog):
    """
    A ttkbootstrap.dialogs Dialog that displays error message

    :return: None
    """
    def __init__(self, title: str, msgs: list[str]):
        dialogs.MessageDialog.__init__(self, 'Errors were encountered', parent=None,
                                       title=title, buttons=['OK'], alert=True)

        self.msgs: list[str] = msgs

    def create_body(self, parent):
        """
        Overrides the create_body method of the base class.  Builds the body of the Dialog

        :param parent: the GUI parent for this Dialog
        :type parent: Tkinter derived Window or Frame
        :return: None
        """

        msg_scroll = scrolled.ScrolledText(parent, padding=5, height=len(self.msgs))
        for msg in self.msgs:
            msg_scroll.insert(END, f'{msg}\n')
        msg_scroll.pack(side=LEFT)


dp_widget_union = BodyWeightWidget | BodyTempWidget | BloodPressureWidget | BloodGlucoseWidget | PulseWidget


specs_map_type = Union[imp.ImportSpecsCollection, exp.ExportSpecsCollection]
spec_type = Union[imp.ImportSpec, exp.ExportSpec]


class ImportExportSpecsListWidget(ttkb.Toplevel):
    """
    A dialog that prompts the user to select an Import or Export Spec for a list of Specs
    """
    def __init__(self, title: str, specs_map: specs_map_type, cancel_action: Callable, ok_action: Callable):
        """
        Creates an instance of ImportExportSpecsListWidget

        :param title: a title for the dialog
        :type title: str
        :param specs_map: an instance of ImportSpecsCollection or ExportSpecsCollection.  The selection list will
        be built for the keys of the dict contained in the specified instance
        :type specs_map: Union[biometrics_tracker.model.importers.ImportSpecsCollection,
        biometrics_tracker.model.exporters.ExportSpecsCollection
        :param cancel_action: a callback to be invoked when the Cancel button is clicked
        :type cancel_action: Callable
        :param ok_action: a callback to be invoked when an item in the listbox is clicked
        :type ok_action: Callable

        """
        ttkb.Toplevel.__init__(self, title=title, iconphoto=pathlib.Path(utilities.whereami('biometrics_tracker'), 'gui',
                               'biometrics_tracker.png').__str__())
        self.spec_map: specs_map_type = specs_map
        self.cancel_action = cancel_action
        self.ok_action = ok_action
        self.spec_id_list: list[str] = self.spec_map.id_list()
        self.selected_spec: Optional[spec_type] = None

        width = 450
        height = 350
        screenwidth = self.winfo_screenwidth()
        screenheight = self.winfo_screenheight()
        alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
        self.geometry(alignstr)

        ttkb.Label(self, text='Select a Specification').grid(column=0, row=0, columnspan=2, sticky=tk.NW, padx=5, pady=5)
        self.listbox: tk.Listbox = tk.Listbox(self, width=50)
        for idx, spec_id in enumerate(self.spec_id_list):
            self.listbox.insert(idx, spec_id)
        self.listbox.bind('<Double-1>', self.ok_action)
        self.listbox.grid(column=0, row=1, columnspan=2, sticky=tk.NW, padx=5, pady=5)
        self.cancel_button = ttkb.Button(self, text='Cancel', width=15, command=cancel_action)
        self.cancel_button.grid(column=0, row=2, sticky=tk.NW, padx=5, pady=5)
        self.grid()

    def show(self) -> None:
        """
        Displays the dialog

        :return: None

        """
        self.focus_set()
        self.grab_set()
        self.wait_window()

    def get_selected_id(self) -> str:
        """
        Returns the select Spec ID

        :return: the selected ImportSpec or ExportSpec ID
        :rtype: str

        """
        idx, = self.listbox.curselection()
        self.selected_spec = self.spec_map.get_spec(self.spec_id_list[idx])
        return self.selected_spec.id


class ImportExportSaveSpecWidget(ttkb.Toplevel):
    """
    A dialog displays a list of existing ImportSpec or ExportSpec ID's and prompts the user for the ID of
    the spec about to be persisted

    """
    def __init__(self, title: str, spec_map: specs_map_type, cancel_action: Callable, save_action: Callable):
        ttkb.Toplevel.__init__(self, title=title, iconphoto=pathlib.Path(utilities.whereami('biometrics_tracker'), 'gui',
                               'biometrics_tracker.png').__str__())
        self.spec_map: specs_map_type = spec_map
        self.cancel_action = cancel_action
        self.save_action = save_action
        self.spec_id_list: list[str] = self.spec_map.id_list()

        width = 450
        height = 300
        screenwidth = self.winfo_screenwidth()
        screenheight = self.winfo_screenheight()
        alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
        self.geometry(alignstr)
        self.columnconfigure(0, weight=33)
        self.columnconfigure(1, weight=66)

        ttkb.Label(self, text='Current Specifications').grid(column=0, row=0, columnspan=2, sticky=tk.NW, padx=5, pady=5)
        self.current_specs = scrolled.ScrolledText(self, height=5, width=60)
        for id in self.spec_id_list:
            self.current_specs.insert(END, id)
        self.current_specs.grid(column=0, row=1, columnspan=2, sticky=tk.NW, padx=5, pady=5)
        ttkb.Label(self, text='ID for New Spec:').grid(column=0, row=2, sticky=tk.NW, padx=5, pady=5)
        self.spec_id_var = ttkb.StringVar()
        self.spec_id_entry = ttkb.Entry(self, textvariable=self.spec_id_var, width=40)
        self.spec_id_entry.grid(column=1, row=2, columnspan=2, sticky=tk.NW, padx=5, pady=5)
        self.cancel_button = ttkb.Button(self, text='Cancel', width=15, command=self.cancel_action)
        self.cancel_button.grid(column=0, row=3, sticky=tk.NW, padx=5, pady=5)
        self.ok_button = ttkb.Button(self, text='OK', width=15, command=self.save_action)
        self.ok_button.grid(column=1, row=3, sticky=tk.NW, padx=5, pady=5)

    def show(self) -> None:
        """
        Dislays the dialog

        :return: None

        """
        self.focus_set()
        self.grab_set()
        self.wait_window()

    def get_spec_id(self) -> str:
        """
        Returns the entered ID
        :return: the entered ID
        :rtype: str

        """
        return self.spec_id_var.get()


class ImportExportSpecButtons(ttkb.Frame):
    """
    This widget contains a pair of buttons, one which invokes the display of an ImportExportSpecsListWidget and
    one which invokes the display of an ImportExportSaveSpecWidget

    """
    def __init__(self, parent, button_title: str, select_title: str, select_action: Callable,
                 save_title: str, save_action: Callable, specs_map: specs_map_type
                 ):
        ttkb.Frame.__init__(self, parent)
        self.select_title: str = select_title
        self.select_action: Callable = select_action
        self.save_title: str = save_title
        self.save_action: Callable = save_action
        self.specs_map: specs_map_type = specs_map
        ttkb.Label(self, text=button_title).grid(column=0, row=1, columnspan=2, padx=2, pady=2)
        self.select_button = ttkb.Button(self, text='Select', command=self.select_specs)
        self.select_button.grid(column=0, row=0, padx=5, pady=2, sticky=tk.NW)
        self.save_button = ttkb.Button(self, text='Save', command=self.save_specs)
        self.save_button.grid(column=1, row=0, padx=5, pady=2, sticky=NE)
        self.select_list: Optional[ImportExportSpecsListWidget] = None
        self.save_widget: Optional[ImportExportSaveSpecWidget] = None

    def destroy_spec_list(self):
        """
        Destroy the currently displayed ImportExportSpecsListWidget

        :return: None

        """
        self.select_list.destroy()

    def destroy_save_widget(self):
        """
        Destroy the currently displayed ImportExportSaveSpecWidget

        :return: None

        """
        self.save_widget.destroy()

    def select_specs(self):
        """
        Display an ImportExportSpecsListWidget.
        :return: None

        """
        self.select_list = ImportExportSpecsListWidget(title=self.select_title, specs_map=self.specs_map,
                                                       cancel_action=self.destroy_spec_list,
                                                       ok_action=self.select_action)
        self.select_list.show()

    def get_selected_spec_id(self) -> str:
        """
        Retrieve the id of the Spec selected on the ImportExportSpecsListWidget dialog

        :return: the selected ID
        :rtype: str

        """
        return self.select_list.get_selected_id()

    def save_specs(self):
        """
        Display an ImportExportSaveSpecWidget dialog

        :return: None

        """
        self.save_widget = ImportExportSaveSpecWidget(title=self.save_title, spec_map=self.specs_map,
                                                      cancel_action=self.destroy_save_widget,
                                                      save_action=self.save_action)
        self.save_widget.show()

    def get_saved_spec_id(self) -> str:
        """
        Retrieve the ID entered to an ImportExportSaveSpec dialog

        :return: the entered ID
        :rtype: str

        """
        id = self.save_widget.get_spec_id()
        self.save_widget.destroy()
        return id


if __name__ == '__main__':

    window = ttkb.Window(title='test plugin widgets', themename='darkly')
    window.mainloop()
    pass

