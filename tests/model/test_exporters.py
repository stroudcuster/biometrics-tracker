import csv
from datetime import datetime
import logging
import logging.config
import pathlib
import pytest
import re
import sqlite3 as sql
from time import sleep
from typing import Callable, Optional

import biometrics_tracker.config.logging_config as log_cfg
import biometrics_tracker.ipc.messages as msgs
import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.exporters as exp
from biometrics_tracker.model.importers import DATE, TIME
import biometrics_tracker.model.persistence as per
import biometrics_tracker.utilities.utilities as util

from tests.model.datapoints_fixtures import people_fix, datapoints_fix, person_data_fix, person_fix, \
    tracking_config_fix, schedule_fix, blood_pressure_data_fix, blood_glucose_data_fix, pulse_data_fix, \
    body_weight_data_fix, body_temp_data_fix, blood_glucose_dp_data_fix, blood_pressure_dp_data_fix, \
    pulse_dp_data_fix, body_temp_dp_data_fix, body_weight_dp_data_fix


from tests.model.random_data import BiometricsRandomData
from tests.model.test_tools import decode_bloodpressure
from tests.test_tools import DATE_FMT, attr_error, dict_diff, decode_date, decode_time, decode_decimal, decode_int

KEY_DATETIME_FMT = f'{DATE_FMT} %H:%M:%S'
PERSON_ID = 'PERSON_ID'


class TestExporterBase:
    """
    A base class to be subclassed by classes that test specific exporter classes
    """
    def __init__(self, temp_dir: pathlib.Path, date_fmt: str, time_fmt: str, uom_handling: exp.UOMHandlingType,
                 person: dp.Person, datapoints_fix):
        """
        Creates and instance of TestExporterBase

        :param temp_dir:  a temporary directory path where the input database, exported files and the logs will be placed
        :type temp_dir: pathlib.Path
        :param date_fmt: the date format spec that will be used when formatting dates in the export data
        :type date_fmt: str
        :param time_fmt: the time format spec that will be used when formatting times in the export data
        :type time_fmt: str
        :param uom_handling: the UOM handling policy; exclude, append to value or place in separate column
        :type uom_handling: biometrics_tracker.model.exporters.UOMHandlingType
        :param person: the Person object associated with the DataPoints to be exported
        :type person: biometrics_tracker.model.datapoints.Person
        :param datapoints_fix: the pytest fixture that will supply the DataPoints to be exported
        :type datapoints_fix: pytest.Fixture

        """
        self.temp_dir: pathlib.Path = temp_dir
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
        self.db = per.DataBase(self.db_path.__str__(), self.queue_mgr, block_req_queue=False)
        self.db.start()

    def create_db_connection(self):
        """
        Create a database connection in biometrics_tracker.model.persistence.DataBase.  Setting the closed property
        to true causes the run routine to exit after creating the connection.

        :return: None

        """
        self.db.closed = True
        self.db.run()

    def send_db_close_msg(self):
        """
        Send a close message to the biometrics_tracker.model.persistence.DataBase instance.  This methold must
        be used to close the database when it is being run in a separate thread

        :return: None

        """
        self.queue_mgr.send_db_req_msg(msgs.CloseDataBaseReqMsg(destination=per.DataBase, replyto=None))

    def insert_datapoints(self, datapoints_fix) -> tuple[dict[str, dp.DataPoint], datetime, datetime]:
        """
        Using the DataPoints fixture to provide data, insert DataPoint rows into the test database

        :param datapoints_fix: a fixture that will provide biometrics_tracker.model.datapionts.DataPoint objects
        :type datapoints_fix: collections.NamedTuple
        :return: A map containing the inserted DataPoints, keyed by a string containing the Person ID, Taken datetime and the DataPointType.name value
        :rtype: tuple[dict[str, biometrics_tracker.model.datapoints.DataPoint], datetime, datetime]

        """
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

    def check_export_init(self, exporter: exp.ExporterBase, person: dp.Person, completion_dest: Callable):
        """
        Assures that the initialization of the Exporter object resulted in appropriately set properties

        :param exporter: the exporter instance to be checked
        :type exporter: biometrics_tracker.model.exporters.ExporterBase
        :param person: the Person instance associated with the DataPoints the exporter instance will export
        :type person: biometrics_tracker.model.datapoints.Person
        :param completion_dest: a callback routine to be invoked when an ImportExportCompletionMsg is received
        :type completion_dest: Callable
        :return: None

        """
        assert exporter.person == person, f'{self.__class__.__name__} init mismatch ' \
                                              f'{attr_error("Person:", str(person), str(exporter.person))}'
        assert exporter.date_fmt == self.date_fmt, f'{self.__class__.__name__} init mismatch ' \
                                                   f'{attr_error("Date Fmt:", self.date_fmt, exporter.date_fmt)}'
        assert exporter.time_fmt == self.time_fmt, f'{self.__class__.__name__} init mismatch ' \
                                                   f'{attr_error("Time Fmt:", self.time_fmt, exporter.time_fmt)}'
        assert exporter.uom_handling == self.uom_handling, \
               f'{self.__class__.__name__} init mismatch {attr_error("UOM Handling:", self.uom_handling.name,exporter.uom_handling)}'
        # This comparison doesn't work for the SQLiteExporter because the test indexed_fields dict has to include an
        # entry for the Person ID, while the one passed to the SQLiteExporter doesn't include this field because
        # the field is added to the list in the exporter code
        if exporter.__class__ != exp.SQLiteExporter:
            assert exporter.indexed_fields == self.indexed_fields, \
                f'{self.__class__.__name__} init mismatch {dict_diff("Indexed Fields:", self.indexed_fields, exporter.indexed_fields)}'
        expected = completion_dest.__name__
        observed = exporter.completion_destination.__name__
        assert exporter.completion_destination == completion_dest, \
            f'{self.__class__.__name__} init mismatch {attr_error("Completion Dest", expected, observed)}'
        self.logger.debug(f'Init test passed for ID {person.id}')

    def check_row(self, row_nbr: int, row):
        """
        Check the column values in an exported row to assure that there are the expected number of rows, that the
        data corresponds to one of the DataPoint instances that were exported, and that the column values match
        those of the corresponding DataPoint instance

        :param row_nbr: The row number withing the export file
        :type row_nbr: int
        :param row: a tuple of column values
        :type row: tuple
        :return: None

        """
        row_count: int = 0
        taken_date = decode_date(self.logger, self.date_fmt, row[self.indexed_fields[DATE]])
        taken_time = decode_time(self.logger, self.time_fmt, row[self.indexed_fields[TIME]])
        taken: datetime = util.mk_datetime(taken_date, taken_time)
        for col_idx, col_data in enumerate(row):
            row_count += 1
            if isinstance(self.fields[col_idx], dp.DataPointType) and len(col_data.strip()) > 0:
                key = f'{self.person.id}:{taken.strftime(KEY_DATETIME_FMT)}:{self.fields[col_idx].name}'
                self.logger.debug(f'Row No.: {row_nbr} Key: {key}')
                assert key in self.datapoints_map, f'{self.__class__.__name__} no DataPoint for {key}'
                datapoint: dp.DataPoint = self.datapoints_map[key]
                assert datapoint.type == self.fields[col_idx], \
                    f'{self.__class__.__name__} {attr_error("Datapoint Type", datapoint.type.name, self.fields[col_idx].name)}'
                uom_str: str = ''
                match datapoint.type:
                    case dp.DataPointType.BP:
                        obs_systolic, obs_diastolic, obs_uom_str = decode_bloodpressure(col_data)
                        systolic_err = attr_error('Systolic', datapoint.data.systolic, obs_systolic)
                        diastolic_err = attr_error('Diastolic', datapoint.data.diastolic, obs_diastolic)
                        assert obs_systolic == datapoint.data.systolic and \
                               obs_diastolic == datapoint.data.diastolic, \
                               f'CSVExporter {systolic_err} {diastolic_err}'
                    case dp.DataPointType.BODY_WGT:
                        observed, uom_str = decode_decimal(col_data)
                        error = attr_error('BodyWeight', datapoint.data.value, observed)
                        if not abs(observed - datapoint.data.value) <= 0.1:
                            self.logger.error(error)
                        assert abs(observed - datapoint.data.value) <= 0.1, f'{self.__class__.__name__} {error}'
                    case dp.DataPointType.BODY_TEMP:
                        observed, uom_str = decode_decimal(col_data)
                        error = attr_error('Body Temperature', datapoint.data.value, observed)
                        if not abs(observed - datapoint.data.value) <= 0.1:
                            self.logger.error(error)
                        assert abs(observed - datapoint.data.value) <= 0.1, f'{self.__class__.__name__} {error}'
                    case dp.DataPointType.PULSE:
                        observed, uom_str = decode_int(col_data)
                        error = attr_error('Pulse', datapoint.data.value, observed)
                        assert observed == datapoint.data.value, f'{self.__class__.__name__} {error}'
                    case dp.DataPointType.BG:
                        observed, uom_str = decode_int(col_data)
                        error = attr_error('Blood Glucose', datapoint.data.value, observed)
                        assert observed == datapoint.data.value, f'{self.__class__.__name__} {error}'


@pytest.mark.DataBase
@pytest.mark.DataPoint
class TestCSVExporter(TestExporterBase):
    """
    Tests the biometrics_tracker.model.exporters.CSVExporter class.  See the doc comment for the parent class for
    details

    """
    def __init__(self, temp_dir: pathlib.Path, date_fmt: str, time_fmt: str, uom_handling: exp.UOMHandlingType,
                 person: dp.Person, datapoints_fix):
        """
        Creates and instance of TestCSVExporter

        :param temp_dir:  a temporary directory path where the input database, exported files and the logs will be placed
        :type temp_dir: pathlib.Path
        :param date_fmt: the date format spec that will be used when formatting dates in the export data
        :type date_fmt: str
        :param time_fmt: the time format spec that will be used when formatting times in the export data
        :type time_fmt: str
        :param uom_handling: the UOM handling policy; exclude, append to value or place in separate column
        :type uom_handling: biometrics_tracker.model.exporters.UOMHandlingType
        :param person: the Person object associated with the DataPoints to be exported
        :type person: biometrics_tracker.model.datapoints.Person
        :param datapoints_fix: the pytest fixture that will supply the DataPoints to be exported
        :type datapoints_fix: pytest.Fixture

        """
        TestExporterBase.__init__(self, temp_dir, date_fmt, time_fmt, uom_handling, person, datapoints_fix)

    def create_field_map(self):
        """
        Creates a list of fields and a map with field names as keys and column indexes as values.  These are used
        to validate the row contents from the exported file

        :return: None

        """
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

    def check_csv_export(self, msg: msgs.ImportExportCompletionMsg, include_header: bool):
        """
        A callback method to be invoked on receipt of a ImportExportCompletionMsg

        :param msg: a message containing info on the success or failure of the export process
        :type msg: biometrics_tracker.ipc.messages.ImportExportCompletionMsg
        :param include_header: should the output csv file include a header row
        :type include_header: bool
        :return: None

        """
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
                    f'Export Column Count mismatch row {row_nbr} {attr_error("Columns", len(self.fields), len(row))}'
                if row_nbr >= 1 or not include_header:
                    self.check_row(row_nbr, row)
        new_path = pathlib.Path(self.temp_dir, f'biometrics-{self.person.id}.csv')
        export_path.rename(new_path.__str__())
        pass

    def test_csv_export(self, temp_dir, completion_dest, include_header: bool, csv_dialect):
        """
        Executes a test run of the export process for a single person

        :param temp_dir:  a temporary directory path where the input database, exported files and the logs will be placed
        :type temp_dir: str
        :param completion_dest: the callback method to be invoked when an ImportExportCompletionMsg is received
        :type completion_dest: Callable
        :param include_header: should the output csv file include a header row
        :type include_header: bool
        :param csv_dialect: the CSV dialect to be used in creating the export CSV file
        :type csv_dialect: csv.Dialect
        :return: None

        """
        self.include_header = include_header
        self.create_field_map()
        self.logger.debug(f'Creating CSVExporter for ID {self.person.id}')
        csv_exporter: exp.CSVExporter = exp.CSVExporter(self.queue_mgr, self.person, self.start.date(), self.end.date(),
                                                        self.date_fmt, self.time_fmt, self.indexed_fields,
                                                        self.uom_handling, temp_dir, completion_dest, include_header,
                                                        csv_dialect)
        self.check_export_init(csv_exporter, self.person, completion_dest)
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


class TestSQLiteExporter(TestExporterBase):
    """
    Tests the biometrics_tracker.model.exporters.CSVExporter class.  See the doc comment for the parent class for
    details

    """
    def __init__(self, temp_dir: pathlib.Path, date_fmt: str, time_fmt: str, uom_handling: exp.UOMHandlingType,
                 person: dp.Person, datapoints_fix):
        """
        Creates and instance of TestSQLiteExporter

        :param temp_dir:  a temporary directory path where the input database, exported files and the logs will be placed
        :type temp_dir: pathlib.Path
        :param date_fmt: the date format spec that will be used when formatting dates in the export data
        :type date_fmt: str
        :param time_fmt: the time format spec that will be used when formatting times in the export data
        :type time_fmt: str
        :param uom_handling: the UOM handling policy; exclude, append to value or place in separate column
        :type uom_handling: biometrics_tracker.model.exporters.UOMHandlingType
        :param person: the Person object associated with the DataPoints to be exported
        :type person: biometrics_tracker.model.datapoints.Person
        :param datapoints_fix: the pytest fixture that will supply the DataPoints to be exported
        :type datapoints_fix: pytest.Fixture

        """
        TestExporterBase.__init__(self, temp_dir, date_fmt, time_fmt, uom_handling, person, datapoints_fix)
        self.filepath = pathlib.Path(temp_dir, 'export.db')

    def create_field_map(self):
        """
        Creates a list of fields and a map with field names as keys and column indexes as values.  These are used
        to validate the row contents from the exported file.  This class needs a special version of this method
        because the SQLiteExporter class prepends a Person ID column the the columns specified for export by
        the user

        :return: None

        """
        match self.uom_handling:
            # If UOM is excluded or appended make straight list of the date, time and metric columns
            case exp.UOMHandlingType.Excluded | exp.UOMHandlingType.AppendedToValue:
                self.fields = [PERSON_ID,
                               DATE,
                               TIME,
                               dp.DataPointType.BP,
                               dp.DataPointType.PULSE,
                               dp.DataPointType.BG,
                               dp.DataPointType.BODY_WGT,
                               dp.DataPointType.BODY_TEMP]
            #  If UOM is in separate column, include fillers in the field list for the UOMs
            case exp.UOMHandlingType.InSeparateColumn:
                self.fields = [PERSON_ID,
                               DATE,
                               TIME,
                               dp.DataPointType.BP, f'{dp.DataPointType.BP.name} UOM',
                               dp.DataPointType.PULSE, f'{dp.DataPointType.PULSE.name} UOM',
                               dp.DataPointType.BG, f'{dp.DataPointType.BG.name} UOM',
                               dp.DataPointType.BODY_WGT, f'{dp.DataPointType.BODY_WGT.name} UOM',
                               dp.DataPointType.BODY_TEMP, f'{dp.DataPointType.BODY_TEMP.name} UOM']

        self.indexed_fields = {}
        for idx, field in enumerate(self.fields):
            if isinstance(field, str):
                self.indexed_fields[field] = idx
            elif isinstance(field, dp.DataPointType):
                self.indexed_fields[dp.dptype_dp_map[field].label()] = idx

    def check_sqlite_export(self, msg: msgs.ImportExportCompletionMsg):
        """
        A callback method to be invoked on receipt of a ImportExportCompletionMsg

        :param msg: a message containing info on the success or failure of the export process
        :type msg: biometrics_tracker.ipc.messages.ImportExportCompletionMsg
        :return: None

        """
        print(f'Completion Messages for {self.person.id} {self.person.name}')
        self.logger.info(f'Completion Messages for {self.person.id} {self.person.name}')
        for line in msg.messages:
            print(line)
            self.logger.info(line)
        export_path = pathlib.Path(self.temp_dir, f'export.db')
        if export_path.exists():
            self.conn = sql.connect(export_path.__str__())
            cur = self.conn.cursor()
            rows = cur.execute('SELECT * FROM EXPORT WHERE PERSON_ID = ?', (self.person.id, ))
            for row_nbr, row in enumerate(rows):
                for col_data in row:
                    self.logger.debug(f'Line {row_nbr}:{",".join(col for col in row)}')
                    assert len(row) == len(self.fields), \
                        f'Export Column Count mismatch row {row_nbr} {attr_error("Columns", len(self.fields), len(row))}'

                    self.check_row(row_nbr, row)
            new_path = pathlib.Path(self.temp_dir, f'export-{self.person.id}.db')
            export_path.rename(new_path.__str__())
        else:
            error = f'SQLite file {self.filepath.__str__()} not found.'
            self.logger.error(error)
            raise FileNotFoundError(error)

    def test_sqlite_export(self, temp_dir, completion_dest: Callable):
        """
        Executes a test run of the export process for a single person

        :param temp_dir:  a temporary directory path where the input database, exported files and the logs will be placed
        :type temp_dir: pathlib.Path
        :param completion_dest: the callback method to be invoked when an ImportExportCompletionMsg is received
        :type completion_dest: Callable
        :return: None

        """
        self.create_field_map()
        self.logger.debug(f'Creating SQLiteExporter for ID {self.person.id}')
        temp_indexed_fields = {field: idx-1 for field, idx in self.indexed_fields.items() if field != PERSON_ID}
        sql_exporter: exp.SQLiteExporter = \
            exp.SQLiteExporter(queue_mgr=self.queue_mgr, person=self.person, start_date=self.start.date(),
                               end_date=self.end.date(), date_fmt=self.date_fmt, time_fmt=self.time_fmt,
                               indexed_fields=temp_indexed_fields, uom_handling=self.uom_handling,
                               export_dir_path=temp_dir, completion_destination=completion_dest)
        self.check_export_init(sql_exporter, self.person, completion_dest)
        sql_exporter.start()
        msg: Optional[msgs.ImportExportCompletionMsg] = None
        while msg is None:
            self.logger.debug(f'Checking completion queue for ID: {self.person.id}')
            msg = self.queue_mgr.check_completion_queue(block=True)
            if msg is not None and msg.destination is not None:
                msg.destination(msg)
                self.logger.info(f'Completion Message received for ID: {self.person.id}')
            else:
                sleep(5)


@pytest.mark.DataPoint
def test_csv_export(tmpdir, people_fix, datapoints_fix):
    """
    Test the CSVExporter class for a number of people.  This is implemented as a module level function because pytest,
    or Pycharm's pytest framework doesn't seem to be able to properly run tests implemented as bound methods.

    :param tmpdir: the pytest tmpdir fixture - provides a temporary directory for test output files and logs
    :type tmpdir: pytest.Fixture
    :param people_fix: a fixture that provides a set of Person instances
    :type people_fix: list[biometrics_tracker.model.datapoints.Person]
    :param datapoints_fix: a fixture that provides a set of DataPoint instances associated with contents of people_fix
    :type datapoints_fix: collections.NamedTuple
    :return: None

    """
    print(f'Temp Dir: {str(tmpdir)}')
    for person in people_fix:
        date_fmt: str = BiometricsRandomData.random_dict_item(exp.date_formats)[1]
        time_fmt: str = BiometricsRandomData.random_dict_item(exp.time_formats)[1]
        uom_handling: exp.UOMHandlingType = exp.UOMHandlingType(BiometricsRandomData.random_int(1, len(
            exp.UOMHandlingType)))
        include_hdr = BiometricsRandomData.random_bool()
        temp_dir: pathlib.Path = pathlib.Path(tmpdir)
        test_exporter = TestCSVExporter(temp_dir, date_fmt, time_fmt, uom_handling, person, datapoints_fix)
        test_exporter.test_csv_export(temp_dir, test_exporter.check_csv_export, include_hdr, csv.excel)
        test_exporter.send_db_close_msg()


@pytest.mark.DataPoint
def test_sqlite_export(tmpdir, people_fix, datapoints_fix):
    """
    Test the SQLiteExporter class for a number of people.  This is implemented as a module level function because
    pytest,or Pycharm's pytest framework doesn't seem to be able to properly run tests implemented as bound methods.

    :param tmpdir: the pytest tmpdir fixture - provides a temporary directory for test output files and logs
    :type tmpdir: pytest.Fixture
    :param people_fix: a fixture that provides a set of Person instances
    :type people_fix: list[biometrics_tracker.model.datapoints.Person]
    :param datapoints_fix: a fixture that provides a set of DataPoint instances associated with contents of people_fix
    :type datapoints_fix: collections.NamedTuple
    :return: None

    """
    print(f'Temp Dir: {str(tmpdir)}')
    for person in people_fix:
        date_fmt: str = BiometricsRandomData.random_dict_item(exp.date_formats)[1]
        time_fmt: str = BiometricsRandomData.random_dict_item(exp.time_formats)[1]
        uom_handling: exp.UOMHandlingType = exp.UOMHandlingType(BiometricsRandomData.random_int(1, len(
            exp.UOMHandlingType)))
        temp_dir: pathlib.Path = pathlib.Path(tmpdir)
        test_exporter = TestSQLiteExporter(temp_dir, date_fmt, time_fmt, uom_handling, person, datapoints_fix)
        test_exporter.test_sqlite_export(temp_dir, test_exporter.check_sqlite_export)
        test_exporter.send_db_close_msg()

