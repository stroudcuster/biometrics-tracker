import collections
import csv
from dataclasses import dataclass
from datetime import datetime, date, time
from decimal import Decimal
from enum import Enum, auto
from io import IOBase
import pathlib
import pickle
import sqlite3 as sql
import threading
from typing import Callable, Iterator, Optional, Union

from biometrics_tracker.model.importers import DATE, TIME, NOTE
import biometrics_tracker.ipc.messages as messages
import biometrics_tracker.ipc.queue_manager as queue_mgr
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.persistence as per
import biometrics_tracker.model.uoms as uoms
from biometrics_tracker.utilities import utilities as utilities


date_formats: dict[str, str] = {'MM-DD-YYYY': '%m-%d-%Y',
                                'MM/DD/YYYY': '%m/%d/%Y',
                                'DD-MM-YYYY': '%d-%m-%Y',
                                'DD/MM/YYYY': '%d/%m/%Y',
                                'YYYY/MM/DD': '%Y/%m/%d',
                                'YYYY-MM-DD': '%Y-%m-%d'}

date_format_strft_map: dict[str, str] = {strft: label for label, strft in date_formats.items()}


time_formats: dict[str, str] = {'HH:MM:SS-24': '%H:%M:%S',
                                'HH:MM:SS-AMPM': '%I:%M:%S %p'}

time_format_strft_map: dict[str, str] = {strft: label for label, strft in time_formats.items()}


class ExportType(Enum):
    CSV = auto()
    SQL = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields ExportType.CSV, ExportType.SQL, etc
        :rtype: biometrics_tracker.model.exporters.ExportType

        """
        for i in range(1, ExportType.__len__() + 1):
            yield ExportType(i)


export_type_name_map = {e.name: e for e in ExportType}


class UOMHandlingType(Enum):
    AppendedToValue = auto()
    InSeparateColumn = auto()
    Excluded = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields UOMHandlingType.AppendedToValue, UOMHandlingType.InSeparateColumn, etc
        :rtype: biometrics_tracker.model.exporters.UOMHandlingType

        """
        for i in range(1, UOMHandlingType.__len__() + 1):
            yield UOMHandlingType(i)


uom_handling_type_name_map = {h.name: h for h in UOMHandlingType}


class ExporterBase(threading.Thread):
    """
    A base class for classes that produce export files from the Biometrics Tracking database
    """
    def __init__(self, export_type: ExportType, queue_mgr: queue_mgr.Queues, person: dp.Person, start_date: date,
                 end_date: date, indexed_fields: dict[str, int], date_fmt: str, time_fmt: str,
                 uom_handling: UOMHandlingType, export_dir_path: pathlib.Path, file_name: str,
                 completion_destination: Optional[Callable]):
        """
        Create and instance of model.exporters.ExporterBase

        :param queue_mgr: an object containing the queues used for inter-thread communication
        :type queue_mgr: biometrics_tracker.ipc.queue_manager.Queues
        :param person: info for the person whose data will be reported
        :type person: biometrics_tracker.model.datapoints.Person
        :param start_date: start of datapoint date range
        :type start_date: datetime.date
        :param end_date: end of datapoint date range
        :type end_date: datetime.date
        :param indexed_fields: the DataPoint properties to be included in the export file
        :type indexed_fields: dict[str, int]

        """
        threading.Thread.__init__(self)
        self.export_type: ExportType = export_type
        self.queue_mgr = queue_mgr
        self.person = person
        self.start_date: date = start_date
        self.end_date: date = end_date
        self.date_fmt = date_fmt
        self.time_fmt = time_fmt
        self.indexed_fields: dict[str, int] = indexed_fields
        self.uom_handling = uom_handling
        self.export_dir_path = export_dir_path
        self.filepath: pathlib.Path = pathlib.Path(export_dir_path, file_name)
        self.completion_destination: Optional[Callable] = completion_destination
        self.open_action_method: Optional[Callable] = None
        self.column_action_method: Optional[Callable] = None
        self.row_action_method: Optional[Callable] = None
        self.close_action_method: Optional[Callable] = None
        self.rpt_cells = collections.OrderedDict()
        self.rows_created: int = 0

    def run(self) -> None:
        """
        This method in invoked when the thread is started.  It sends a request to the persistence manager to
        retrieve the datapoints for the specified person

        :return: None

        """
        start_datetime = utilities.mk_datetime(self.start_date, time(hour=0, minute=0, second=0))
        end_datetime = utilities.mk_datetime(self.end_date, time(hour=23, minute=59, second=59))
        self.queue_mgr.send_db_req_msg(messages.DataPointReqMsg(destination=per.DataBase, replyto=self.produce_export,
                                                                person_id=self.person.id, start=start_datetime,
                                                                end=end_datetime,
                                                                operation=messages.DBOperation.RETRIEVE_SET))
        resp: messages.DataPointRespMsg = self.queue_mgr.check_db_resp_queue(block=True)
        if resp is not None and resp.destination is not None:
            resp.destination(resp)

    def fmt_date(self, dt: date):
        """
        Formats a date using the format selected by the user

        :param dt: The date to be formated
        :type dt: datetime.date
        :return: A formatted string date
        :rtype: str

        """
        return dt.strftime(self.date_fmt)

    def fmt_time(self, tm: time):
        """
        Formats a time using the format selected by the user

        :param tm: The time to be formated
        :type tm: datetime.time
        :return: A formatted string time
        :rtype: str

        """
        return tm.strftime(self.time_fmt)

    def fmt_value(self, value: Union[int, Decimal, tuple[int, int]], uom: uoms.UOM) -> str:
        """
        Formats a metric value, appending the u.o.m. if requested by the user

        :param value: a tuple of (systolic, diastolic) for blood pressure, a single numeric value for all others
        :type value: Union[int, Decimal, tuple[int, int]]
        :param uom: the UOM associated with the metrics
        :type uom: biometrics_tracker.model.uoms.UOM
        :return: a formatted string
        :rtype: str

        """
        fmt_value: str = ''
        if isinstance(value, int):
            if self.uom_handling == UOMHandlingType.AppendedToValue:
                fmt_value = f'{value:3d} {uom.abbreviation()}'
            else:
                fmt_value = f'{value:3d}'
        elif isinstance(value, Decimal):
            if self.uom_handling == UOMHandlingType.AppendedToValue:
                fmt_value = f'{value:.1f} {uom.abbreviation()}'
            else:
                fmt_value = f'{value:.1f}'
        else:
            if self.uom_handling == UOMHandlingType.AppendedToValue:
                fmt_value = f'{value[0]:3d}/{value[1]:3d} {uom.abbreviation()}'
            else:
                fmt_value = f'{value[0]:3d}/{value[1]:3d}'
        return fmt_value

    def produce_export(self, msg: messages.DataPointRespMsg):
        """
        Produces the export file, using callbacks specified by subclasses to handle open, column, row and close actions
        :param msg: a response message containing a list of DataPoints
        :type msg: biometrics_tracker.ipc.messages.DataPointRespMsg
        :return: None

        """
        self.open_action_method()
        prev_dp_datetime: Optional[datetime] = None
        for col in range(len(self.indexed_fields)):
            self.rpt_cells[col] = ' '
        row_pending: bool = False
        datapoints: list[dp.dptypes_union] = msg.datapoints
        for datapoint in datapoints:
            dp_date, dp_time = utilities.split_datetime(datapoint.taken)
            if prev_dp_datetime is not None and datapoint.taken != prev_dp_datetime and row_pending:
                self.row_action_method()
                self.rows_created += 1
                row_pending = False
                for col in range(len(self.indexed_fields)):
                    self.rpt_cells[col] = ' '
            prev_dp_datetime = datapoint.taken
            if dp.dptype_dp_map[datapoint.type].label() in self.indexed_fields:
                if DATE in self.indexed_fields:
                    col = self.indexed_fields[DATE]
                    if len(self.rpt_cells[col].strip()) == 0:
                        self.rpt_cells[col] = self.fmt_date(dp_date)
                if TIME in self.indexed_fields:
                    col = self.indexed_fields[TIME]
                    if len(self.rpt_cells[col].strip()) == 0:
                        self.rpt_cells[col] = self.fmt_time(dp_time)
                if NOTE in self.indexed_fields:
                    self.rpt_cells[self.indexed_fields[NOTE]] = datapoint.note
                col: int = self.indexed_fields[dp.dptype_dp_map[datapoint.type].label()]
                if self.uom_handling == UOMHandlingType.InSeparateColumn:
                    self.rpt_cells[col], self.rpt_cells[col+1] = self.column_action_method(datapoint.data)
                else:
                    self.rpt_cells[col] = self.column_action_method(datapoint.data)
                row_pending = True
        if row_pending:
            self.row_action_method()
            self.rows_created += 1
        self.close_action_method()
        msgs: list[str] = [f'Export file {self.filepath.__str__()} Type: {self.export_type.name}'
                           f' rows {self.rows_created} created',
                           f'The exported data was written to file  {self.filepath.__str__()}']
        msg = messages.ImportExportCompletionMsg(destination=self.completion_destination,
                                                 replyto=None, status=messages.Completion.SUCCESS,
                                                 messages=msgs)
        self.queue_mgr.send_completion_msg(msg)


class CSVExporter(ExporterBase):
    def __init__(self, queue_mgr: queue_mgr.Queues, person: dp.Person, start_date: date, end_date: date,
                 date_fmt: str, time_fmt: str, indexed_fields: dict[str, int], uom_handling: UOMHandlingType,
                 export_dir_path: pathlib.Path, completion_destination: Callable, include_header_row: bool,
                 csv_dialect: csv.Dialect = 'excel'):
        """
        Create and instance of model.CSVExporter

        :param queue_mgr: an object containing the queues used for inter-thread communication
        :type queue_mgr: biometrics_tracker.ipc.queue_manager.Queues
        :param person: info for the person whose data will be reported
        :type person: biometrics_tracker.model.datapoints.Person
        :param start_date: start of datapoint date range
        :type start_date: datetime.date
        :param end_date: end of datapoint date range
        :type end_date: datetime.date
        :param date_fmt: the strftime format string to be used for dates
        :type date_fmt: str
        :param time_fmt: the strftime format string to be used for times
        :type time_fmt: str
        :param indexed_fields: the DataPoint properties to be included in the export file
        :type indexed_fields: dict[str, int]
        :param uom_handling: should the u.o.m. appended to values, placed in a separate column or ignored
        :type  uom_handling: UOMHandlingType
        :param export_dir_path: a Path object pointing to the directory where the export file will be created
        :type export_dir_path: pathlib.Path
        :param completion_destination: the method to be used for the destination of the completion message
        :type completion_destination: Callable
        :param include_header_row:  should the output file include a header row
        :type include_header_row: bool

        """

        ExporterBase.__init__(self,  ExportType.CSV, queue_mgr, person, start_date, end_date, indexed_fields, date_fmt, time_fmt,
                              uom_handling, export_dir_path, 'biometrics.csv', completion_destination)
        self.include_header_row: bool = include_header_row
        self.csv_dialect: csv.Dialect = csv_dialect
        self.file: Optional[IOBase] = None
        self.open_action_method = self.create_csv_file
        self.column_action_method = self.column_action
        self.row_action_method = self.row_action
        self.close_action_method = self.close_csv_file
        self.csv_writer: Optional[csv.writer] = None

    def create_csv_file(self):
        """
        Handle the open action.  Creates a text file and, if specified, writes a header row to the CSV file

        :return: None

        """
        self.file = self.filepath.open(mode='w', newline='')
        
        self.csv_writer = csv.writer(self.file, dialect=self.csv_dialect)
        if self.include_header_row:
            self.csv_writer.writerow([f for f in self.indexed_fields.keys()])

    def close_csv_file(self):
        """
        Handle the close action.  Closes the export file

        :return: None

        """
        self.file.close()

    def column_action(self, data):
        """
        Handles the column action.  Formats the value, and if specified, the u.o.m.  If the u.o.m. is to
        be put in its own row, a tuple containing the formatted metric value and the u.o.m. is returned.  Otherwise
        a single string is returned.

        :param data: a instance of one of the metrics classes
        :type data: biometrics_tracker.model.datapoints.BloodPressure, Pulse, BloodGlucose, etc
        :return: None

        """
        match data.__class__:
            case dp.BloodPressure:
                if self.uom_handling == UOMHandlingType.InSeparateColumn:
                    return self.fmt_value(value=(data.systolic, data.diastolic), uom=data.uom), data.uom.abbreviation()
                else:
                    return self.fmt_value(value=(data.systolic, data.diastolic), uom=data.uom)
            case _:
                if self.uom_handling == UOMHandlingType.InSeparateColumn:
                    return self.fmt_value(value=data.value, uom=data.uom), data.uom.abbreviation()
                else:
                    return self.fmt_value(value=data.value, uom=data.uom)

    def row_action(self):
        """
        Handle the row action.  Write a csv file record

        :return: None

        """
        self.csv_writer.writerow([value for value in self.rpt_cells.values()])


class SQLiteExporter(ExporterBase):
    """
    Creates an SQLite databaes file and exports datapoints to it.  The database consists of two tables: PEOPLE and
    EXPORT.
    """
    def __init__(self, queue_mgr: queue_mgr.Queues, person: dp.Person, start_date: date, end_date: date,
                 date_fmt: str, time_fmt: str, indexed_fields: dict[str, int], uom_handling: UOMHandlingType,
                 export_dir_path: pathlib.Path, completion_destination: Callable):
        """
        Create and instance of model.CSVExporter

        :param queue_mgr: an object containing the queues used for inter-thread communication
        :type queue_mgr: biometrics_tracker.ipc.queue_manager.Queues
        :param person: info for the person whose data will be reported
        :type person: biometrics_tracker.model.datapoints.Person
        :param start_date: start of datapoint date range
        :type start_date: datetime.date
        :param end_date: end of datapoint date range
        :type end_date: datetime.date
        :param indexed_fields: the DataPoint properties to be included in the export file
        :type indexed_fields: dict[str, int]
        :param date_fmt: the strftime format string to be used for dates
        :type date_fmt: str
        :param time_fmt: the strftime format string to be used for times
        :type time_fmt: str
        :param uom_handling: should the u.o.m. appended to values, placed in a separate column or ignored
        :type  uom_handling: UOMHandlingType
        :param export_dir_path: a Path object pointing to the directory where the export file will be created
        :type export_dir_path: pathlib.Path
        :param completion_destination: the method to be used for the destination of the completion message
        :type completion_destination: Callable

        """
        ExporterBase.__init__(self, ExportType.SQL, queue_mgr, person, start_date, end_date, indexed_fields, date_fmt,
                              time_fmt, uom_handling, export_dir_path, 'biometrics.db', completion_destination)
        self.filepath = pathlib.Path(export_dir_path, 'export.db')
        self.open_action_method = self.create_sqlite_db
        self.column_action_method = self.column_action
        self.row_action_method = self.insert_data
        self.close_action_method = self.close_sqlite_db
        self.rpt_cells = collections.OrderedDict()
        self.conn = None
        self.insert_person_stmt: str = 'INSERT INTO PERSON(ID, NAME, DOB) VALUE(?, ?, ?)'
        self.insert_data_stmt: str = ''

    def create_sqlite_db(self):
        """
        Handles the open action.  Creates the database file, inserts a single row into the PEOPLE table and builds
         an insert statement to be used to insert rows into the EXPORT table.

        :return: None

        """
        if self.filepath.exists():
            self.filepath.unlink()
        self.conn = sql.connect(self.filepath.__str__())
        cur = self.conn.cursor()
        cur.execute('CREATE TABLE PEOPLE (ID PRIMARY KEY, NAME, DOB DATE);')
        cur.execute('INSERT INTO PEOPLE (ID, NAME, DOB) VALUES(?, ?, ?);', (self.person.id, self.person.name,
                                                                         self.fmt_date(self.person.dob)))
        cur.execute('COMMIT;')
        fields = ', '.join(field.upper().replace(' ', '_') for field in self.indexed_fields.keys())
        create = f'CREATE TABLE EXPORT (PERSON_ID, {fields});'
        cur.execute(create)
        values = ', ?'*len(self.indexed_fields)
        insert_tail = f'{fields}) VALUES(?{values});'
        self.insert_data_stmt = f'INSERT INTO EXPORT (PERSON_ID, {insert_tail}'

    def close_sqlite_db(self):
        """
        Handles the close action.  Closes the connection to the export database

        :return: None

        """
        self.conn.close()

    def column_action(self, data):
        """
        Handle the column action.  Formats the value, and if specified, the u.o.m.  If the u.o.m. is to
        be put in its own row, a tuple containing the formatted metric value and the u.o.m. is returned.  Otherwise
        a single string is returned.


        :param data: a instance of one of the metrics classes
        :type data: biometrics_tracker.model.datapoints.BloodPressure, Pulse, BloodGlucose, etc
        :return: None

        """
        match data.__class__:
            case dp.BloodPressure:
                if self.uom_handling == UOMHandlingType.InSeparateColumn:
                    return self.fmt_value(value=(data.systolic, data.diastolic), uom=data.uom), data.uom.abbreviation()
                else:
                    return self.fmt_value(value=(data.systolic, data.diastolic), uom=data.uom)
            case _:
                if self.uom_handling == UOMHandlingType.InSeparateColumn:
                    return self.fmt_value(value=data.value, uom=data.uom), data.uom.abbreviation()
                else:
                    return self.fmt_value(value=data.value, uom=data.uom)

    def insert_data(self):
        """
        Handles the row action.  Inserts a row into the EXPORT table

        :return: None

        """
        curs = self.conn.cursor()
        values: list[str] = [self.person.id]
        values.extend(self.rpt_cells.values())
        curs.execute(self.insert_data_stmt, tuple(values))
        curs.execute('COMMIT;')


@dataclass
class ExportSpec:
    """
    Used to persist the entry values of the biometrics_tracker.gui.tk_gui.ExportFrame for reuse.

    """
    id: str
    export_type: ExportType
    include_header_row: bool
    uom_handling_type: UOMHandlingType
    date_format: str
    time_format: str
    column_types: dict[int, str]


class ExportSpecsCollection:
    """
    Used to persist a number of instances of ExportSpec, keyed by the ExportSpec.id property

    """
    def __init__(self):
        self.map: dict[str, ExportSpec] = {}

    def get_spec(self, id: str) -> Optional[ExportSpec]:
        if id in self.map:
            return self.map[id]
        else:
            return None

    def add_spec(self, specs: ExportSpec):
        self.map[specs.id] = specs

    def id_list(self) -> list[str]:
        return [key for key in self.map.keys()]


class ExportSpecsPickler:
    """
    Provides pickling and unpickling for an instance of ExportSpecsCollection
    """

    FILENAME: str = 'ExportSpecsCollection'

    def __init__(self, pickle_path: pathlib.Path):
        """
        Creates an instance of ExportSpecsPickler

        :param pickle_path: a path to the directory where the pickle file will be placed or is located
        :type pickle_path: pathlib.Path

        """
        self.pickle_path: pathlib.Path = pathlib.Path(pickle_path, ExportSpecsPickler.FILENAME)

    def save(self, specs: ExportSpecsCollection):
        """
        Pickles the provided ExportSpecsCollection instance

        :param specs: the ExportFrame enty values to be preserved for later use
        :type specs: biometrics_tracker.model.exporters.ExportSpecsCollection
        :return: None

        """
        with self.pickle_path.open(mode="wb") as pp:
            try:
                pickle.Pickler(pp).dump(specs)
            except pickle.PicklingError:
                ...

    def retrieve(self) -> Optional[ExportSpecsCollection]:
        """
        Unpickles and returns an instance of ExportSpecsCollection

        :return: biometrics_tracker.model.exporters.ExportSpecsCollection
        """
        if self.pickle_path.exists():
            with self.pickle_path.open(mode="rb") as pp:
                try:
                    return pickle.Unpickler(pp).load()
                except pickle.PicklingError:
                    return None
        else:
            return ExportSpecsCollection()







