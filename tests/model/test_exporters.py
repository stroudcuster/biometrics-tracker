import csv
from datetime import date, time, datetime
from decimal import Decimal
import pathlib
import pytest
import re
from typing import Any, Callable, Optional

from datapoints_fixtures import person_data_fix, person_fix, tracking_config_fix, schedule_fix, \
    blood_pressure_data_fix, blood_glucose_data_fix, pulse_data_fix, body_weight_data_fix, body_temp_data_fix, \
    blood_glucose_dp_data_fix, blood_pressure_dp_data_fix, pulse_dp_data_fix, body_temp_dp_data_fix, \
    body_weight_dp_data_fix


import biometrics_tracker.ipc.messages as msgs
import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.exporters as exp
from biometrics_tracker.model.importers import DATE, TIME, NOTE
import biometrics_tracker.model.persistence as per
import biometrics_tracker.utilities.utilities as util

from tests.model.random_data import BiometricsRandomData
from tests.test_tools import DATETIME_FMT, DATE_FMT, TIME_FMT, attr_error, compare_hms, compare_mdyhms, \
    compare_object, dict_diff


@pytest.mark.DataBase
@pytest.mark.DataPoint
class TestExporters:
    def __init__(self, temp_dir, person: dp.Person, datapoints_fix):
        self.temp_dir = temp_dir
        self.queue_mgr = queues.Queues(sleep_seconds=2)
        self.random_data = BiometricsRandomData()
        self.db_path = pathlib.Path(self.temp_dir, 'biometrics.db')
        self.now: datetime = datetime.now()
        self.db = per.DataBase(self.db_path.__str__(), self.queue_mgr, block_req_queue=True)
        self.create_db_connection()
        self.db.create_db()
        self.datapoints_map: dict[str, dp.DataPoint] = {}
        self.start: Optional[datetime] = None
        self.end: Optional[datetime] = None
        self.fields: list[str | dp.DataPointType] = [DATE,
                                  TIME,
                                  dp.dptype_dp_map[dp.DataPointType.BP],
                                  dp.dptype_dp_map[dp.DataPointType.PULSE],
                                  dp.dptype_dp_map[dp.DataPointType.BG],
                                  dp.dptype_dp_map[dp.DataPointType.BODY_WGT],
                                  dp.dptype_dp_map[dp.DataPointType.BODY_TEMP]]
        self.indexed_fields: dict[str, int] = {}
        for idx, field in enumerate(self.fields):
            if isinstance(field, str):
                self.indexed_fields[field] = idx
            elif isinstance(field, dp.DataPointType):
                self.indexed_fields[dp.dptype_dp_map[field].label] = idx
        self.datapoints_map, self.start, self.end = self.insert_datapoints(datapoints_fix)
        self.date_re = re.compile('')
        self.mdy_re = re.compile('"*(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})"*')
        self.ymd_re = re.compile('"*(\d{2,4})[/-](\d{1,2})[/-](\d{1,2})"*')
        self.time12_re = re.compile('"*(\d{1,2}):(\d{2}):*(\d{0,2})\s*([am|AM|pm|PM]{2})"*')
        self.time24_re = re.compile('"*(\d{1,2}):(\d{2}):*(\d{0,2})\s"*')
        self.bp_re = re.compile('"*(\d{2,3})/(\d{2,3})([\s\w])*"*')
        self.int_value_re = re.compile('"*(\d{2,4})([\s\w]*)"*')
        self.dec_value_re = re.compile('"*(\d{2,4}[.]*\d*)([\s\w]*)"*')

    def decode_date(self, date_fmt: str, date_str: str) -> date:
        if date_fmt[0:2] == '%m':
            groups = self.mdy_re.match(date_str)
            return date(year=groups[2], month=groups[0], day=groups[1])
        else:
            groups = self.ymd_re.match(date_str)
            return date(year=groups[0], month=groups[1], day=groups[2])

    def decode_time(self, time_fmt: str, time_str: str) -> time:
        if time_fmt[0:2] == '%H':
            groups = self.time24_re.match(time_str)
            return time(hour=groups[0], minute=groups[1], second=groups[2])
        else:
            groups = self.time12_re.match(time_str)
            hour = groups[0]
            if groups[3] in ['pm', 'PM']:
                hour += 12
            return time(hour=hour, minute=groups[1], second=groups[2])

    def decode_int(self, int_str: str) -> tuple[int, str]:
        groups = self.int_value_re.match(int_str)
        uom_str = ''
        if groups is None:
            return 0, uom_str
        elif len(groups) > 1:
            uom_str = groups[1]
        return int(groups[0]), uom_str

    def decode_decimal(self, dec_str: str) -> tuple[Decimal, str]:
        groups = self.dec_value_re.match(dec_str)
        uom_str: str = ''
        if groups is None:
            return Decimal(0), uom_str
        elif len(groups) > 1:
            uom_str = groups[1]
        return Decimal(float(groups[0])), uom_str

    def decode_bloodpressure(self, bp_str: str) -> tuple[int, int, str]:
        groups = self.bp_re.match(bp_str)
        uom_str: str = ''
        if groups is None:
            return 0, 0, ''
        elif len(groups) > 2:
            uom_str = groups[2]
        return int(groups[0]), int(groups[1]), uom_str

    def create_db_connection(self):
        self.db.closed = True
        self.db.run()

    def close_db_connection(self):
        self.db.close_db()

    def db_open_do_close(self, do_action: Callable, do_action_args: Optional[dict] = None):
        self.create_db_connection()
        rtn_value: Any = None
        if do_action_args is None:
            rtn_value = do_action()
        else:
            rtn_value = do_action(do_action_args)
        self.close_db_connection()
        return rtn_value

    def insert_datapoints(self, datapoints_fix) -> tuple[dict[str, dp.DataPoint], datetime, datetime]:
        def make_key() -> str:
            return f'{datapoint.person_id}:{datapoint.taken.strftime(DATETIME_FMT)}:{datapoint.type.name}'
        datapoints_map: dict[str, dp.DataPoint] = {}
        first_taken: Optional[datetime] = None
        last_taken: Optional[datetime] = None
        for datapoint in datapoints_fix:
            self.db.insert_datapoint(datapoint)
            datapoints_map[make_key()] = datapoint
            if first_taken is None or datapoint.taken < first_taken:
                first_taken = datapoint.taken
            if last_taken is None or datapoint.taken > last_taken:
                last_taken = datapoint.taken

        return datapoints_map, first_taken, last_taken

    def test_export_init(self, person: dp.Person, date_fmt: str, time_fmt: str, uom_handling: exp.UOMHandlingType,
                         completion_dest: Callable):
        assert self.person == person, f'CSVExporter init mismatch ' \
                                              f'{attr_error("Person:", str(person), str(self.person))}'
        assert self.date_fmt == date_fmt, f'CSVExporter init mismatch ' \
                                                  f'{attr_error("Date Fmt:", date_fmt, self.date_fmt)}'
        assert self.time_fmt == time_fmt, f'CSVExporter init mismatch ' \
                                          f'{attr_error("Time Fmt:", time_fmt, self.time_fmt)}'
        assert self.uom_handling == uom_handling, \
            f'CSVExporter init mismatch {attr_error("UOM Handling:", uom_handling.name,self.uom_handling)}'
        assert self.indexed_fields == self.indexed_fields, \
            f'CSVExporter init mismatch {dict_diff("Indexed Fields:", self.indexed_fields, self.indexed_fields)}'
        expected = completion_dest.__name__
        observed = self.completion_destination.__name__
        assert self.completion_destination == completion_dest, \
            f'CSVExporter init mismatch {attr_error("Completion Dest", expected, observed)}'

    def check_csv_export(self):
        export_path = pathlib.Path(self.temp_dir, 'biometrics.csv')
        with export_path.open(mode='r') as ep:
            reader = csv.reader(ep, delimiter=',')
            row_count: int = 0
            for row_nbr, row in enumerate(reader):
                if row_nbr >= 1:
                    row_count += 1
                assert len(row) == len(self.fields), \
                    f'CSV Export Column Count mismatch row {row_nbr} {attr_error("Columns", len(self.fields), len(row))}'
                taken_date = self.decode_date(row[self.indexed_fields[DATE]])
                taken_time = self.decode_time(row[self.indexed_fields[TIME]])
                taken: datetime = util.mk_datetime(taken_date, taken_time)
                for col_idx, col_data in enumerate(row):
                    if isinstance(self.fields[col_idx], dp.DataPointType):
                        key = f'{self.person.id}:{taken.strftime("%m/%d/%Y %H:%M:%S")}:{self.fields[col_idx].name}'
                        assert key in self.datapoints_map, f'CSVExporter no DataPoint for {key}'
                        datapoint: dp.DataPoint = self.datapoints_map[key]
                        uom_str: str = ''
                        match datapoint.__class__:
                            case dp.BloodPressure:
                                obs_systolic, obs_diastolic, obs_uom_str = self.decode_bloodpressure(col_data)
                                exp_systolic = datapoint.data.systolic
                                exp_
                                assert systolic == datapoint.data.systolic and diastolic == datapoint.data.diastolic,
                                    f'CSVExporter {attr_error("Systolic", , systolic)} {attr_error("Diastolic", datapoint.data)}'
                            case dp.BodyWeight:
                                ...
                            case dp.BodyTemperature:
                                ...
                            case dp.Pulse | dp.BloodGlucose:
                                ...



    def test_csv_export(self, temp_dir, person: dp.Person, date_fmt: str, time_fmt: str,
                        uom_handling: exp.UOMHandlingType, completion_dest, include_header: bool,
                        csv_dialect):

        csv_exporter: exp.CSVExporter = exp.CSVExporter(self.queue_mgr, person, self.start.date(), self.end.date(),
                                                        date_fmt, time_fmt, self.indexed_fields, uom_handling, temp_dir,
                                                        completion_dest, include_header, csv_dialect)
        self.test_export_init(person, date_fmt, time_fmt, uom_handling, completion_dest)
        expected = str(include_header)
        observed = str(csv_exporter.include_header_row)
        assert csv_exporter.include_header_row == include_header, \
            f'CSVExporter init mismatch {attr_error("Inc Header Row", expected, observed)}'
        expected = str(csv_dialect)
        observed = str(csv_exporter.csv_dialect)
        assert csv_exporter.csv_dialect == csv_dialect, \
            f'CSVExporter init mismatch {attr_error("CSV Dialect", expected, observed)}'
        csv_exporter.start()

    def test_sql_export(self, temp_dir, person: dp.Person, date_fmt: str, time_fmt: str,
                        uom_handling: exp.UOMHandlingType, completion_dest: Callable):
        csv_exporter: exp.SQLiteExporter = \
            exp.SQLiteExporter(self.queue_mgr, person, self.start.date(), self.end.date(), date_fmt, time_fmt,
                               self.indexed_fields, uom_handling, temp_dir,completion_dest)
        self.test_export_init()


def test_csv_export(temp_dir, people_fix, datapoints_fix):

    for person in people_fix:
        date_fmt: str = BiometricsRandomData.random_dict_item(exp.date_formats)[1]
        time_fmt: str = BiometricsRandomData.random_dict_item(exp.time_formats)[1]
        uom_handling: exp.UOMHandlingType = exp.UOMHandlingType(BiometricsRandomData.random_int(1, len(
            exp.UOMHandlingType)))
        include_hdr = BiometricsRandomData.random_bool()
        test_exporter = TestExporters(temp_dir, person, datapoints_fix)
        test_exporter.test_csv_export(temp_dir, person, date_fmt, time_fmt, uom_handling, test_exporter.check_csv_export,
                                      include_hdr, csv.excel)
