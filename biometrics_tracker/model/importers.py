import csv
from dataclasses import dataclass
from datetime import date, time
from decimal import Decimal
import pathlib
import pickle
import re
import threading
from typing import Callable, Optional

import biometrics_tracker.ipc.messages as messages
import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.persistence as per
from biometrics_tracker.utilities.utilities import mk_datetime

IGNORE: str = 'Ignore'
DATE: str = 'Date'
TIME: str = 'Time'
BLOOD_PRESSURE: str = dp.BloodPressure.label()
BLOOD_GLUCOSE: str = dp.BloodGlucose.label()
BODY_WEIGHT: str = dp.BodyWeight.label()
BODY_TEMP: str = dp.BodyTemperature.label()
PULSE: str = dp.Pulse.label()
NOTE: str = 'Note'


class CSVImporter(threading.Thread):
    """
    Implements the import process for a CSV file
    """
    def __init__(self, queue_mgr: queues.Queues, person: dp.Person, filename: str, header_rows: int,
                 use_prev_row_date: bool, indexed_fields: dict[int, str], completion_dest: Callable):
        """
        Create an instance of CSVImporter

        :param queue_mgr: the message queue manager
        :type queue_mgr: biometrics_tracker.ipc.queue_mgr.Queues
        :param person: an instance of the model.Person class represent the person for whom the data is being imported.
        :type person: biometrics_tracker.model.datapoints.Person
        :param filename: the path name of the file from which the data will be imported
        :type filename: str
        :param header_rows: the number of header rows to be ignored
        :type header_rows: int
        :param use_prev_row_date: if True, use the previous row's date for rows with a blank date column
        :type use_prev_row_date: bool
        :param indexed_fields: a dict where the key is an int representing the position of the field and the
               value is the value returned by the label method of the class (e.g. model.BodyWeight, model.Pulse)
               implementing a record of a type of reading.
        :type indexed_fields: dict[int, str]
        :param completion_dest: the method to be used as the destination for the completion message
        :type completion_dest: Callable

        """
        threading.Thread.__init__(self)
        self.queue_mgr = queue_mgr
        self.person: dp.Person = person
        self.filename: str = filename
        self.header_rows = header_rows
        self.use_prev_row_date = use_prev_row_date
        self.indexed_fields: dict[int, str] = indexed_fields
        self.max_index: int = len(indexed_fields.keys())
        self.completion_dest = completion_dest

    def run(self) -> None:
        """
        Read the CSV file and save the resulting data to the database

        :return: None

        """
        with open(self.filename, mode='rt', newline='') as file:
            reader = csv.reader(file, delimiter=',')
            dp_count: int = 0
            err_count: int = 0
            row_count: int = 0
            msg_list: list[str] = []
            date_re = re.compile('["]*(\d{1,2})/(\d{1,2})/(\d{2,4})["]*')
            time_re = re.compile('["]*(\d{1,2})[:](\d{2})[:]*(\d{0,2})\s*([am|AM|pm|PM]*)["]*')
            bp_re = re.compile('["]*(\d{2,3})/(\d{2,3})[\s\w]*["]*')
            int_value_re = re.compile('["]*(\d{2,4})[\s\w]*["]*')
            dec_value_re = re.compile('["]*(\d{2,4}[.]*\d*)[\s\w]*["]*')
            prev_dp_date: Optional[date] = None
            for row_nbr, row in enumerate(reader):
                if row_nbr >= self.header_rows:
                    row_count += 1
                    dp_date: Optional[date] = None
                    dp_time: time = time(hour=0, minute=0, second=0)
                    dp_note: str = ' '
                    data: list = []
                    for col_nbr, field in enumerate(row):
                        if col_nbr < self.max_index and len(field.strip()) > 0:
                            indexed_field = self.indexed_fields[col_nbr+1]
                            if indexed_field == DATE:
                                try:
                                    re_result = date_re.match(field)
                                    if re_result is not None and len(re_result.groups()) == 3:
                                        m, d, y = re_result.groups()
                                        month = int(m)
                                        day = int(d)
                                        year = int(y)
                                        if year < 100:
                                            year += 2000
                                        dp_date = date(year, month, day)
                                    else:
                                        raise ValueError
                                except ValueError:
                                    msg_list.append(f'Value "{field}" in row {row_nbr+1}, column {col_nbr+1} can not be'
                                                    f'converted to a date.')
                                    err_count += 1
                            elif indexed_field == TIME:
                                try:
                                    re_result = time_re.match(field)
                                    if re_result is not None and len(re_result.groups()) == 4:
                                        h, m, s, ampm = re_result.groups()
                                        hour = int(h)
                                        minute = int(m)
                                        second = int(s)
                                        if ampm in ['pm', 'PM']:
                                            hour += 12
                                        elif len(ampm.strip()) > 0 and ampm not in ['am', 'AM']:
                                            raise ValueError
                                        dp_time = time(hour, minute, second)
                                    else:
                                        raise ValueError
                                except ValueError:
                                    msg_list.append(f'Value "{field}" in row {row_nbr+1}, column {col_nbr+1} can not be'
                                                    f'converted to a time.')
                                    err_count += 1
                            elif indexed_field == BLOOD_PRESSURE:
                                if self.person.is_tracked(dp.DataPointType.BP):
                                    try:
                                        re_result = bp_re.match(field)
                                        if re_result is not None and len(re_result.groups()) == 2:
                                            s, d = re_result.groups()
                                            systolic = int(s)
                                            diastolic = int(d)
                                            bp = dp.BloodPressure(systolic, diastolic,
                                                                  self.person.dp_type_uom(dp.DataPointType.BP))
                                            data.append(bp)
                                        else:
                                            raise ValueError
                                    except ValueError:
                                        msg_list.append(f'Value "{field}" in row {row_nbr+1}, column {col_nbr+1} can not'
                                                        f' be converted to a blood pressure.')
                                        err_count += 1
                            elif indexed_field == BLOOD_GLUCOSE:
                                if self.person.is_tracked(dp.DataPointType.BG):
                                    try:
                                        re_result = int_value_re.match(field)
                                        if re_result is not None and len(re_result.groups()) > 0:
                                            value = int(re_result.group(1))
                                            bg = dp.BloodGlucose(value, self.person.dp_type_uom(dp.DataPointType.BG))
                                            data.append(bg)
                                        else:
                                            raise ValueError
                                    except ValueError:
                                        msg_list.append(f'Value "{field}" in row {row_nbr + 1}, column {col_nbr + 1} can not be'
                                                        f'converted to in blood glucose')
                                        err_count += 1
                            elif indexed_field == BODY_WEIGHT:
                                if self.person.is_tracked(dp.DataPointType.BODY_WGT):
                                    try:
                                        re_result = dec_value_re.match(field)
                                        if re_result is not None and len(re_result.groups()) > 0:
                                            value = Decimal(re_result.group(1))
                                            wgt = dp.BodyWeight(value, self.person.dp_type_uom(dp.DataPointType.BODY_WGT))
                                            data.append(wgt)
                                        else:
                                            raise ValueError
                                    except ValueError:
                                        msg_list.append(f'Value "{field}" in row {row_nbr + 1}, column {col_nbr + 1}'
                                                        f' can not be converted to a weight.')
                                        err_count += 1
                            elif indexed_field == BODY_TEMP:
                                if self.person.is_tracked(dp.DataPointType.BODY_TEMP):
                                    try:
                                        re_result = dec_value_re.match(field)
                                        if re_result is not None and len(re_result.groups()) > 0:
                                            value = Decimal(re_result.group(1))
                                            temp = dp.BodyTemperature(value,
                                                                      self.person.dp_type_uom(dp.DataPointType.BODY_TEMP))
                                            data.append(temp)
                                        else:
                                            raise ValueError
                                    except ValueError:
                                        msg_list.append(f'Value "{field}" in row {row_nbr+1}, column {col_nbr+1} can not'
                                                        f' be converted to a temperature.')
                                        err_count += 1
                            elif indexed_field == PULSE:
                                if self.person.is_tracked(dp.DataPointType.PULSE):
                                    try:
                                        re_result = int_value_re.match(field)
                                        if re_result is not None and len(re_result.groups()) > 0:
                                            value = int(re_result.group(1))
                                            pulse = dp.Pulse(value, self.person.dp_type_uom(dp.DataPointType.PULSE))
                                            data.append(pulse)
                                        else:
                                            raise ValueError
                                    except ValueError:
                                        msg_list.append(f'Value "{field}" in row {row_nbr + 1}, column {col_nbr + 1} can'
                                                        f' not be converted to a pulse.')
                                        err_count += 1
                            elif indexed_field == IGNORE:
                                pass
                            else:
                                if len(field) > 0:
                                    msg_list.append(
                                                    f'Value "{field}" in at row {row_nbr}, column {col_nbr+1} '
                                                    f'not classified.')
                    if dp_date is None and self.use_prev_row_date:
                        dp_date = prev_dp_date
                    else:
                        prev_dp_date = dp_date
                    if dp_date is not None:
                        for datum in data:
                            dattim = mk_datetime(dp_date, dp_time)
                            match datum.__class__.__name__:
                                case dp.BloodPressure.__name__:
                                    datapoint: dp.BloodPressureDP = dp.BloodPressureDP(self.person.id, dattim,
                                                                                       dp_note, datum)
                                    dp_count += 1
                                case dp.BloodGlucose.__name__:
                                    datapoint: dp.BloodGlucoseDP = dp.BloodGlucoseDP(self.person.id, dattim,
                                                                                     dp_note, datum)
                                    dp_count += 1
                                case dp.BodyWeight.__name__:
                                    datapoint: dp.BodyWeightDP = dp.BodyWeightDP(self.person.id, dattim, dp_note,
                                                                                 datum)
                                    dp_count += 1
                                case dp.BodyTemperature.__name__:
                                    datapoint: dp.BodyTemperatureDP = dp.BodyTemperatureDP(self.person.id, dattim,
                                                                                           dp_note, datum)
                                    dp_count += 1
                                case dp.Pulse.__name__:
                                    datapoint: dp.PulseDP = dp.PulseDP(self.person.id, dattim, dp_note, datum)
                                    dp_count += 1
                                case _:
                                    pass
                            self.queue_mgr.send_db_req_msg(messages.DataPointMsg(destination=per.DataBase,
                                                                                 replyto=None,
                                                                                 payload=datapoint,
                                                                                 operation=messages.DBOperation.
                                                                                 INSERT))
                    else:
                        msg_list.append(f'No date found for row {row_nbr}. No datapoints were created.')
                        err_count += 1
            msg_list.append(f'Created {dp_count} datapoints from {row_count} rows with {err_count} errors.')
            self.queue_mgr.send_completion_msg(messages.ImportExportCompletionMsg(destination=self.completion_dest,
                                                                                  replyto=None,
                                                                                  status=messages.Completion.SUCCESS,
                                                                                  messages=msg_list))

@dataclass
class ImportSpec:
    """
    Used to persist the entry values from an instance of biometrics_tracker.gui.tk_gui.CSVImportFrame for reuse

    """
    id: str
    nbr_of_header_rows: int
    use_date_from_prev_row: bool
    column_types: dict[int, str]


class ImportSpecsCollection:
    """
    Used to persist a number of instances of ImportSpec, keyed by the ImportSpec.id property

    """
    def __init__(self):
        self.map: dict[str, ImportSpec] = {}

    def get_spec(self, id: str) -> Optional[ImportSpec]:
        if id in self.map:
            return self.map[id]
        else:
            return None

    def add_spec(self, specs: ImportSpec):
        self.map[specs.id] = specs

    def id_list(self) -> list[str]:
        return [key for key in self.map.keys()]


class ImportSpecsPickler:
    """
    Provides pickling and unpickling for an instance of ImportSpecsCollection
    """

    FILENAME: str = 'ImportSpecsCollection'

    def __init__(self, pickle_path: pathlib.Path):
        """
        Creates an instance of ImportSpecsPickler

        :param pickle_path: a path to the directory where the pickle file will be placed or is located
        :type pickle_path: pathlib.Path

        """
        self.pickle_path: pathlib.Path = pathlib.Path(pickle_path, ImportSpecsPickler.FILENAME)

    def save(self, specs: ImportSpecsCollection):
        """
        Pickles the provided ImportSpecsCollection instance

        :param specs: the CSVImportFrame enty values to be preserved for later use
        :type specs: biometrics_tracker.model.importers.ImportSpecsCollection
        :return: None

        """
        with self.pickle_path.open(mode="wb") as pp:
            try:
                pickle.Pickler(pp).dump(specs)
            except pickle.PicklingError:
                ...

    def retrieve(self) -> Optional[ImportSpecsCollection]:
        """
        Unpickles and returns an instance of ImportSpecsCollection

        :return: biometrics_tracker.model.importers.ImportSpecsCollection
        """
        if self.pickle_path.exists():
            with self.pickle_path.open(mode="rb") as pp:
                try:
                    return pickle.Unpickler(pp).load()
                except pickle.PicklingError:
                    return None
        else:
            return ImportSpecsCollection()
