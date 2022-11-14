import csv
from datetime import date, time, datetime
from decimal import Decimal
import logging
import logging.config
import pathlib
import pytest
import re
from time import sleep
from typing import Any, Callable, Optional

from datapoints_fixtures import people_fix, blood_pressure_data_fix, blood_glucose_data_fix, pulse_data_fix, \
    body_weight_data_fix, body_temp_data_fix, blood_glucose_dp_data_fix, blood_pressure_dp_data_fix, \
    pulse_dp_data_fix, body_temp_dp_data_fix, body_weight_dp_data_fix, datapoints_fix


import biometrics_tracker.config.logging_config as log_cfg
import biometrics_tracker.ipc.messages as msgs
import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.exporters as exp
from biometrics_tracker.model.importers import DATE, TIME, NOTE
import biometrics_tracker.model.persistence as per
import biometrics_tracker.utilities.utilities as util

from tests.model.random_data import BiometricsRandomData
from tests.test_tools import DATE_FMT, attr_error, dict_diff

KEY_DATETIME_FMT = f'{DATE_FMT} %H:%M:%S'

@pytest.mark.DataBase
@pytest.mark.DataPoint
class TestExporters:
    def __init__(self, temp_dir, date_fmt: str, time_fmt: str, uom_handling: exp.UOMHandlingType, person: dp.Person,
                 datapoints_fix):
        self.temp_dir: str = temp_dir
        self.date_fmt: str = date_fmt
        self.time_fmt: str = time_fmt
        self.uom_handling: exp.UOMHandlingType = uom_handling
        self.person = person
        self.queue_mgr = queues.Queues(sleep_seconds=2)
        self.random_data = BiometricsRandomData()
        self.db_path = pathlib.Path(self.temp_dir, 'biometrics.db')
        self.now: datetime = datetime.now()
        logging_config = log_cfg.LoggingConfig()
        logging_config.set_log_filename(pathlib.Path(temp_dir, 'test_exporters.log').__str__())
        logging.config.dictConfig(logging_config.config)
        self.logger = logging.getLogger('unit_tests')
        self.logger.debug('Deleting Database from Previous Run')
        self.db_path.unlink(missing_ok=True)
        self.logger.info('Starting Database')
        self.db = per.DataBase(self.db_path.__str__(), self.queue_mgr, block_req_queue=True)
        self.logger.debug('Getting Database Connection')
        self.create_db_connection()
        self.logger.debug('Creating Tables and Indexes')
        self.db.create_db()
        self.logger.info('Creating DataPoints')
        self.datapoints_map: dict[str, dp.DataPoint] = {}
        self.start: Optional[datetime] = None
        self.end: Optional[datetime] = None
        self.datapoints_map, self.start, self.end = self.insert_datapoints(datapoints_fix)
        self.db.close()
        self.fields: list[str | dp.DataPointType] = []
        self.indexed_fields: dict[str, int] = {}
        self.date_re = re.compile('')
        self.mdy_re = re.compile('"*(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})"*')
        self.ymd_re = re.compile('"*(\d{2,4})[/-](\d{1,2})[/-](\d{1,2})"*')
        self.time12_re = re.compile('"*(\d{1,2}):(\d{2}):(\d{0,2})\s*([am|AM|pm|PM]{2})"*')
        self.time24_re = re.compile('"*(\d{1,2}):(\d{2}):(\d{0,2})\s*"*')
        self.bp_re = re.compile('"*(\d{2,3})/(\d{2,3})([\s\w])*"*')
        self.int_value_re = re.compile('"*(\d{2,4})([\s\w]*)"*')
        self.dec_value_re = re.compile('"*(\d{2,4}[.]*\d*)([\s\w]*)"*')
        self.db = per.DataBase(self.db_path.__str__(), self.queue_mgr, block_req_queue=False)
        self.db.start()

    def create_field_map(self):
        match self.uom_handling:
            # If UOM is excluded or appended make straight list of the date, time and metric columns
            case exp.UOMHandlingType.Excluded | exp.UOMHandlingType.AppendedToValue:
                self.fields = [DATE,
                               TIME,
                               dp.DataPointType.BP,
                               dp.DataPointType.PULSE,
                               dp.DataPointType.BG,
                               dp.DataPointType.BODY_WGT,
                               dp.DataPointType.BODY_TEMP]
            #  If UOM is in separate column, include fillers in the field list for the UOMs
            case exp.UOMHandlingType.InSeparateColumn:
                self.fields = [DATE,
                               TIME,
                               dp.DataPointType.BP, f'{dp.DataPointType.BP.name} UOM',
                               dp.DataPointType.PULSE, f'{dp.DataPointType.PULSE.name} UOM',
                               dp.DataPointType.BG, f'{dp.DataPointType.BG.name} UOM',
                               dp.DataPointType.BODY_WGT, f'{dp.DataPointType.BODY_WGT.name} UOM',
                               dp.DataPointType.BODY_TEMP, f'{dp.DataPointType.BODY_TEMP.name} UOM',]

        self.indexed_fields = {}
        for idx, field in enumerate(self.fields):
            if isinstance(field, str):
                self.indexed_fields[field] = idx
            elif isinstance(field, dp.DataPointType):
                self.indexed_fields[dp.dptype_dp_map[field].label()] = idx

    def decode_date(self, date_fmt: str, date_str: str) -> Optional[date]:
        try:
            if date_fmt[0:2] in ['%m', '%d']:
                match = self.mdy_re.match(date_str)
                if match is not None:
                    groups = match.groups()
                    if date_fmt[0:2] == '%m':
                        return date(year=int(groups[2]), month=int(groups[0]), day=int(groups[1]))
                    else:
                        return date(year=int(groups[2]), month=int(groups[1]), day=int(groups[0]))
            else:
                match = self.ymd_re.match(date_str)
                if match is not None:
                    groups = match.groups()
                    return date(year=int(groups[0]), month=int(groups[1]), day=int(groups[2]))
            raise ValueError(f'Date: {date_str} could not be decoded using format {date_fmt}')
        except ValueError:
            error_msg: str = f'Date: {date_str} could not be decoded using format {date_fmt}'
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def decode_time(self, time_fmt: str, time_str: str) -> Optional[time]:
        try:
            if time_fmt[0:2] == '%H':
                match = self.time24_re.match(time_str)
                if match is not None:
                    groups = match.groups()
                    return time(hour=int(groups[0]), minute=int(groups[1]), second=int(groups[2]))
            else:
                match = self.time12_re.match(time_str)
                if match is not None:
                    groups = match.groups()
                    hour = int(groups[0])
                    if len(groups) == 4 and hour < 12 and groups[3] in ['pm', 'PM']:
                        hour += 12
                    return time(hour=hour, minute=int(groups[1]), second=int(groups[2]))
            raise ValueError(f'Time: {time_str} could not be decoded using format {time_fmt}')
        except ValueError:
            error_msg: str = f'Time: {time_str} could not be decoded using format {time_fmt}'
            self.logger.error(error_msg)
            raise ValueError(error_msg)

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

    def send_db_close_msg(self):
        self.queue_mgr.send_db_req_msg(msgs.CloseDataBaseReqMsg(destination=per.DataBase, replyto=None))

    def db_open_do_close(self, do_action: Callable, do_action_args: Optional[dict] = None):
        self.create_db_connection()
        rtn_value: Any = None
        if do_action_args is None:
            rtn_value = do_action()
        else:
            rtn_value = do_action(do_action_args)
        self.db.close()
        return rtn_value

    def insert_datapoints(self, datapoints_fix) -> tuple[dict[str, dp.DataPoint], datetime, datetime]:
        def make_key() -> str:
            return f'{datapoint.person_id}:{datapoint.taken.strftime(KEY_DATETIME_FMT)}:{datapoint.type.name}'
        datapoints_map: dict[str, dp.DataPoint] = {}
        first_taken: Optional[datetime] = None
        last_taken: Optional[datetime] = None
        person_id: Optional[str] = None
        for datapoint in datapoints_fix:
            self.db.insert_datapoint(datapoint)
            datapoints_map[make_key()] = datapoint
            if first_taken is None or datapoint.taken < first_taken:
                first_taken = datapoint.taken
            if last_taken is None or datapoint.taken > last_taken:
                last_taken = datapoint.taken
            if person_id is None:
                person_id = datapoint.person_id

        self.logger.info(f'{len(datapoints_map)} inserted for ID: {person_id}')
        return datapoints_map, first_taken, last_taken

    def test_export_init(self, exporter: exp.ExporterBase, person: dp.Person, completion_dest: Callable):
        assert exporter.person == person, f'CSVExporter init mismatch ' \
                                              f'{attr_error("Person:", str(person), str(exporter.person))}'
        assert exporter.date_fmt == self.date_fmt, f'CSVExporter init mismatch ' \
                                                   f'{attr_error("Date Fmt:", self.date_fmt, exporter.date_fmt)}'
        assert exporter.time_fmt == self.time_fmt, f'CSVExporter init mismatch ' \
                                                   f'{attr_error("Time Fmt:", self.time_fmt, exporter.time_fmt)}'
        assert exporter.uom_handling == self.uom_handling, \
               f'CSVExporter init mismatch {attr_error("UOM Handling:", self.uom_handling.name,exporter.uom_handling)}'
        assert exporter.indexed_fields == self.indexed_fields, \
            f'CSVExporter init mismatch {dict_diff("Indexed Fields:", self.indexed_fields, exporter.indexed_fields)}'
        expected = completion_dest.__name__
        observed = exporter.completion_destination.__name__
        assert exporter.completion_destination == completion_dest, \
            f'CSVExporter init mismatch {attr_error("Completion Dest", expected, observed)}'
        self.logger.debug(f'Init test passed for ID {person.id}')

    def check_csv_export(self, msg: msgs.ImportExportCompletionMsg, include_header: bool):
        print(f'Completion Messages for {self.person.id} {self.person.name}')
        self.logger.info(f'Completion Messages for {self.person.id} {self.person.name}')
        for line in msg.messages:
            print(line)
            self.logger.info(line)
        export_path = pathlib.Path(self.temp_dir, f'biometrics.csv')
        with export_path.open(mode='r') as ep:
            self.logger.info(f'Checking Export File {export_path.__str__()}')
            reader = csv.reader(ep, delimiter=',')
            row_count: int = 0
            for row_nbr, row in enumerate(reader):
                self.logger.debug(f'Line {row_nbr}:{",".join(col for col in row)}')
                assert len(row) == len(self.fields), \
                    f'CSV Export Column Count mismatch row {row_nbr} {attr_error("Columns", len(self.fields), len(row))}'
                if row_nbr >= 1 or not include_header:
                    row_count += 1
                    taken_date = self.decode_date(self.date_fmt, row[self.indexed_fields[DATE]])
                    taken_time = self.decode_time(self.time_fmt, row[self.indexed_fields[TIME]])
                    taken: datetime = util.mk_datetime(taken_date, taken_time)
                    for col_idx, col_data in enumerate(row):
                        if isinstance(self.fields[col_idx], dp.DataPointType) and len(col_data.strip()) > 0:
                            key = f'{self.person.id}:{taken.strftime(KEY_DATETIME_FMT)}:{self.fields[col_idx].name}'
                            self.logger.debug(f'Key: {key}')
                            assert key in self.datapoints_map, f'CSVExporter no DataPoint for {key}'
                            datapoint: dp.DataPoint = self.datapoints_map[key]
                            assert datapoint.type == self.fields[col_idx], \
                                f'CSVExporter {attr_error("Datapoint Type", datapoint.type.name, self.field_list[col_idx].name)}'
                            uom_str: str = ''
                            match datapoint.__class__:
                                case dp.BloodPressure:
                                    obs_systolic, obs_diastolic, obs_uom_str = self.decode_bloodpressure(col_data)
                                    systolic_err = attr_error('Systolic', datapoint.data.systolic, obs_systolic)
                                    diastolic_err = attr_error('Diastolic', datapoint.data.diastolic, obs_diastolic)
                                    assert obs_systolic == datapoint.data.systolic and \
                                        obs_diastolic == datapoint.data.diastolic, \
                                        f'CSVExporter {systolic_err} {diastolic_err}'
                                case dp.BodyWeight:
                                    observed, uom_str = self.decode_decimal(col_data)
                                    error = attr_error('BodyWeight', datapoint.data.value, observed)
                                    assert observed == datapoint.data.value, f'CSVExporter {error}'
                                case dp.BodyTemperature:
                                    observed, uom_str = self.decode_decimal(col_data)
                                    error = attr_error('Body Temperature', datapoint.data.value.observed)
                                    assert observed == datapoint.data.value, f'CSVExporter {error}'
                                case dp.Pulse:
                                    observed, uom_str = self.decode_int(col_data)
                                    error = attr_error('Pulse', datapoint.data.value, observed)
                                    assert observed == datapoint.data.value, f'CSVExporter {error}'
                                case dp.BloodGlucose:
                                    observed, uom_str = self.decode_int(col_data)
                                    error = attr_error('Blood Glucose', datapoint.data.value, observed)
                                    assert observed == datapoint.data.value, f'CSVExporter {error}'
        new_path = pathlib.Path(self.temp_dir, f'biometrics-{self.person.id}.csv')
        export_path.rename(new_path.__str__())
        pass

    def test_csv_export(self, temp_dir, completion_dest, include_header: bool, csv_dialect):
        self.include_header = include_header
        self.create_field_map()
        self.logger.debug(f'Creating CSVExporter for ID {self.person.id}')
        csv_exporter: exp.CSVExporter = exp.CSVExporter(self.queue_mgr, self.person, self.start.date(), self.end.date(),
                                                        self.date_fmt, self.time_fmt, self.indexed_fields,
                                                        self.uom_handling, temp_dir, completion_dest, include_header,
                                                        csv_dialect)
        self.test_export_init(csv_exporter, self.person, completion_dest)
        expected = str(include_header)
        observed = str(csv_exporter.include_header_row)
        assert csv_exporter.include_header_row == include_header, \
            f'CSVExporter init mismatch {attr_error("Inc Header Row", expected, observed)}'
        expected = str(csv_dialect)
        observed = str(csv_exporter.csv_dialect)
        assert csv_exporter.csv_dialect == csv_dialect, \
            f'CSVExporter init mismatch {attr_error("CSV Dialect", expected, observed)}'
        csv_exporter.start()
        msg: Optional[msgs.ImportExportCompletionMsg] = None
        while msg is None:
            self.logger.debug(f'Checking completion queue for ID: {self.person.id}')
            msg = self.queue_mgr.check_completion_queue(block=True)
            if msg is not None and msg.destination is not None:
                msg.destination(msg, include_header)
                self.logger.info(f'Completion Message received for ID: {self.person.id}')
            else:
                sleep(5)

    def test_sql_export(self, temp_dir, person: dp.Person, date_fmt: str, time_fmt: str,
                        uom_handling: exp.UOMHandlingType, completion_dest: Callable):
        csv_exporter: exp.SQLiteExporter = \
            exp.SQLiteExporter(self.queue_mgr, person, self.start.date(), self.end.date(), date_fmt, time_fmt,
                               self.indexed_fields, uom_handling, temp_dir,completion_dest)
        self.test_export_init()


@pytest.mark.DataPoint
def test_csv_export(tmpdir, people_fix, datapoints_fix):
    print(f'Temp Dir: {str(tmpdir)}')
    for person in people_fix:
        date_fmt: str = BiometricsRandomData.random_dict_item(exp.date_formats)[1]
        time_fmt: str = BiometricsRandomData.random_dict_item(exp.time_formats)[1]
        uom_handling: exp.UOMHandlingType = exp.UOMHandlingType(BiometricsRandomData.random_int(1, len(
            exp.UOMHandlingType)))
        include_hdr = BiometricsRandomData.random_bool()
        temp_dir: pathlib.Path = pathlib.Path(tmpdir)
        test_exporter = TestExporters(temp_dir, date_fmt, time_fmt, uom_handling, person, datapoints_fix)
        test_exporter.test_csv_export(temp_dir, test_exporter.check_csv_export, include_hdr, csv.excel)
        test_exporter.send_db_close_msg()


