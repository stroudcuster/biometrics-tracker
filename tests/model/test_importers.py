import csv
from datetime import datetime
import logging
import logging.config
import pathlib
import pytest
import re
from time import sleep
from typing import Callable, Optional

import biometrics_tracker.config.logging_config as log_cfg
import biometrics_tracker.ipc.messages as msgs
import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.importers as imp
import biometrics_tracker.model.persistence as per
import biometrics_tracker.utilities.utilities as util

from tests.model.datapoints_fixtures import people_fix, datapoints_fix, person_data_fix, person_fix, \
    tracking_config_fix, schedule_fix, blood_pressure_data_fix, blood_glucose_data_fix, pulse_data_fix, \
    body_weight_data_fix, body_temp_data_fix, blood_glucose_dp_data_fix, blood_pressure_dp_data_fix, \
    pulse_dp_data_fix, body_temp_dp_data_fix, body_weight_dp_data_fix

from tests.model.random_data import BiometricsRandomData
import tests.test_tools as tt
import tests.model.test_tools as model_tt


KEY_DATETIME_FMT = '%m/%d/%Y %H:%M:%S'
CSV_DATE_FMT = '%m/%d/%Y'
CSV_TIME_FMT = '%H:%M:%S'


class TestImporterBase:
    """
    A base class for classes that contain importer tests

    """
    def __init__(self, temp_dir: pathlib.Path, person: dp.Person,
                 datapoints_fix):
        """
        Creates and instance of TestExporterBase

        :param temp_dir:  a temporary directory path where the input database, exported files and the logs will be placed
        :type temp_dir: pathlib.Path
        :param person: the Person object associated with the DataPoints to be exported
        :type person: biometrics_tracker.model.datapoints.Person
        :param datapoints_fix: the pytest fixture that will supply the DataPoints to be exported
        :type datapoints_fix: pytest.Fixture

        """
        self.temp_dir: pathlib.Path = temp_dir
        self.person = person
        self.queue_mgr = queues.Queues(sleep_seconds=2)
        self.random_data = BiometricsRandomData()
        self.db_path = pathlib.Path(self.temp_dir, f'biometrics-{person.id}.db')
        self.now: datetime = datetime.now()
        self.fields: list[str] = [imp.DATE,
                                  imp.TIME,
                                  imp.BLOOD_PRESSURE,
                                  imp.PULSE,
                                  imp.BLOOD_GLUCOSE,
                                  imp.BODY_WEIGHT,
                                  imp.BODY_TEMP]

        self.field_dptype_map: dict[str, dp.DataPointType] = {
            imp.BLOOD_PRESSURE: dp.DataPointType.BP,
            imp.PULSE: dp.DataPointType.PULSE,
            imp.BLOOD_GLUCOSE: dp.DataPointType.BG,
            imp.BODY_WEIGHT: dp.DataPointType.BODY_WGT,
            imp.BODY_TEMP: dp.DataPointType.BODY_TEMP}

        self.indexed_fields: dict[int, str] = {}
        self.fields_indexed: dict[str, int] = {}
        for idx, field in enumerate(self.fields):
            self.indexed_fields[idx+1] = field
            self.fields_indexed[field] = idx

        logging_config = log_cfg.LoggingConfig()
        logging_config.set_log_filename(pathlib.Path(temp_dir, 'test_importers.log').__str__())
        logging.config.dictConfig(logging_config.config)

        self.logger = logging.getLogger('unit_tests')
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
        self.db.close()
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


class TestCSVImporter(TestImporterBase):
    """
    Tests the TestCSVImporter class

    """
    def __init__(self, temp_dir: pathlib.Path, person: dp.Person, datapoints_fix):
        """
        Creates and instance of TestExporterBase

        :param temp_dir:  a temporary directory path where the input database, exported files and the logs will be placed
        :type temp_dir: pathlib.Path
        :param person: the Person object associated with the DataPoints to be exported
        :type person: biometrics_tracker.model.datapoints.Person
        :param datapoints_fix: the pytest fixture that will supply the DataPoints to be exported
        :type datapoints_fix: pytest.Fixture

        """
        TestImporterBase.__init__(self, temp_dir, person, datapoints_fix)
        self.nbr_of_header_lines: int = self.random_data.random_int(0, 3)
        self.use_prev_row_date: bool = self.random_data.random_bool()
        self.csv_filepath: Optional[pathlib.Path] = None
        self.taken_lower: Optional[datetime] = None
        self.taken_upper: Optional[datetime] = None

    def check_import_init(self, importer: imp.CSVImporter, person: dp.Person, completion_dest: Callable):
        """
        Assures that the initialization of the Exporter object resulted in appropriately set properties

        :param importer: the importer instance to be checked
        :type importer: biometrics_tracker.model.importer.cSVImporter
        :param person: the Person instance associated with the DataPoints the exporter instance will export
        :type person: biometrics_tracker.model.datapoints.Person
        :param completion_dest: a callback routine to be invoked when an ImportExportCompletionMsg is received
        :type completion_dest: Callable
        :return: None

        """
        assert importer.person == person, f'{self.__class__.__name__} init mismatch ' \
                                          f'{tt.attr_error("Person:", str(person), str(importer.person))}'
        assert importer.indexed_fields == self.indexed_fields, \
            f'{self.__class__.__name__} init mismatch {tt.dict_diff("Indexed Fields:", self.indexed_fields, importer.indexed_fields)}'
        expected = completion_dest.__name__
        observed = importer.completion_dest.__name__
        assert importer.completion_dest == completion_dest, \
            f'{self.__class__.__name__} init mismatch {tt.attr_error("Completion Dest", expected, observed)}'
        self.logger.debug(f'Init test passed for ID {person.id}')

    def create_csv(self, person: dp.Person, datapoints_fix) -> tuple[pathlib.Path, datetime, datetime]:
        """
        Create a CSV file from the DataPoints provided by the datapoints_fix fixture

        :param person: the Person object associated with the DataPoints to be written to the CSV file
        :type person: biometrics_tracker.model.datapoints.Person
        :param datapoints_fix: a fixture that provides a list of DataPoints instances
        :type datapoints_fix: collections.NamedTuple
        :return: a Path object of the  CSV file and the boundary values of the DataPoint taken datetime property
        :rtype: tuple[pathlib.Path, datetime, datetime]

        """
        def output_row(csv_row: list[str]):
            if not tt.compare_mdyhms(prev_taken_list[-1:][0], datapoint.taken):
                prev_taken: datetime = prev_taken_list[-1:][0]
                fmt_date = prev_taken.strftime(CSV_DATE_FMT)
                fmt_time = prev_taken.strftime(CSV_TIME_FMT)
                if self.use_prev_row_date and prev_taken.date() == prev_taken_list[-2:][0]:
                    fmt_date = ' '
                csv_row[self.fields_indexed[imp.DATE]] = fmt_date
                csv_row[self.fields_indexed[imp.TIME]] = fmt_time
                csv_writer.writerow(c for c in csv_row)
                prev_taken_list.append(datapoint.taken)

        csv_filepath = pathlib.Path(self.temp_dir, f'import-{person.id}.csv')
        csv_file = csv_filepath.open(mode='w', newline='')
        csv_writer = csv.writer(csv_file, dialect='excel')
        if self.nbr_of_header_lines > 0:
            if self.nbr_of_header_lines > 1:
                for _ in range(0, self.nbr_of_header_lines-1):
                    csv_writer.writerow('.' for f in self.fields_indexed.keys())
            csv_writer.writerow([f for f in self.fields_indexed.keys()])
        prev_taken_list: list[datetime] = []
        csv_row: list[str] = [' ' for field in self.fields]
        taken_lower: Optional[datetime] = None
        taken_upper: Optional[datetime] = None
        for datapoint in datapoints_fix:
            if datapoint.person_id == person.id:
                if len(prev_taken_list) > 0:
                    output_row(csv_row)
                    csv_row = [' ' for field in self.fields]
                else:
                    prev_taken_list.append(datapoint.taken)
                if taken_lower is None or datapoint.taken < taken_lower:
                    taken_lower = datapoint.taken
                if taken_upper is None or datapoint.taken > taken_upper:
                    taken_upper = datapoint.taken
                if not person.is_tracked(datapoint.type):
                    # CSVImporter requires that the Person instance has a TrackingConfig instance for the datapoint
                    # types being imported, so that it can assign appropriate UOMs to the values imported
                    tracking_config = dp.TrackingConfig(datapoint.type, self.random_data.random_uom(datapoint.type),
                                                        tracked=True)
                    person.add_tracked_dp_type(datapoint.type, tracking_config)
                match datapoint.type:
                    case dp.DataPointType.BP:
                        if imp.BLOOD_PRESSURE in self.fields_indexed:
                            csv_row[self.fields_indexed[imp.BLOOD_PRESSURE]] = \
                                f'{datapoint.data.systolic}/{datapoint.data.diastolic}'
                    case dp.DataPointType.PULSE:
                        if imp.PULSE in self.fields_indexed:
                            csv_row[self.fields_indexed[imp.PULSE]] = f'{datapoint.data.value}'
                    case dp.DataPointType.BG:
                        if imp.BLOOD_GLUCOSE in self.fields_indexed:
                            csv_row[self.fields_indexed[imp.BLOOD_GLUCOSE]] = f'{datapoint.data.value}'
                    case dp.DataPointType.BODY_WGT:
                        if imp.BODY_WEIGHT in self.fields_indexed:
                            csv_row[self.fields_indexed[imp.BODY_WEIGHT]] = f'{datapoint.data.value:.1f}'
                    case dp.DataPointType.BODY_TEMP:
                        if imp.BODY_TEMP in self.fields_indexed:
                            csv_row[self.fields_indexed[imp.BODY_TEMP]] = f'{datapoint.data.value:.1f}'
        if len(prev_taken_list) > 0:
            output_row(csv_row)
        csv_file.close()
        return csv_filepath, taken_lower, taken_upper

    def check_import(self, msg: msgs.DataPointRespMsg):
        """
        Compare the contents of the CSV file to the list of DataPoints

        :param msg: a DataPointRespMsg containing a list of DataPoints created by the import process
        :type msg: biometrics_tracker.ipc.messages.DataPointRespMsg
        :return: None

        """
        def make_datapoint_key() -> str:
            return f'{datapoint.person_id}:{datapoint.taken.strftime(KEY_DATETIME_FMT)}:{datapoint.type.name}'

        def make_csv_key(person_id: str, date_str: str, time_str: str, dp_type: dp.DataPointType) -> str:
            taken_date = tt.decode_date(self.logger, CSV_DATE_FMT, date_str)
            taken_time = tt.decode_time(self.logger, CSV_TIME_FMT, time_str)
            taken = util.mk_datetime(taken_date, taken_time)
            return f'{person_id}:{taken.strftime(KEY_DATETIME_FMT)}:{dp_type.name}'

        def error_preamble(row_nbr: int, key: str):
            return f'Row No.: {row_nbr}, Key: {key} '

        datapoints_map: dict[str, dp.DataPoint] = {}
        for datapoint in msg.datapoints:
            datapoints_map[make_datapoint_key()] = datapoint
        self.send_db_close_msg()

        with self.csv_filepath.open() as csvf:
            csv_reader: csv.reader = csv.reader(csvf)
            for row_nbr, row in enumerate(csv_reader):
                if row_nbr >= self.nbr_of_header_lines:
                    taken_date_str = row[self.fields_indexed[imp.DATE]]
                    taken_time_str = row[self.fields_indexed[imp.TIME]]
                    for col_nbr, col_data in enumerate(row):
                        if col_nbr not in [self.fields_indexed[imp.DATE], self.fields_indexed[imp.TIME]] and \
                                len(col_data.strip()) > 0:
                            dp_type: dp.DataPointType = self.field_dptype_map[self.indexed_fields[col_nbr+1]]
                            csv_key = make_csv_key(self.person.id, taken_date_str, taken_time_str, dp_type)
                            assert csv_key in datapoints_map, f'Row: {row_nbr} Col: {col_nbr} Key: {csv_key} not found'
                            datapoint: dp.DataPoint = datapoints_map[csv_key]
                            match dp_type:
                                case dp.DataPointType.BP:
                                    systolic, diastolic, uom = model_tt.decode_bloodpressure(row[col_nbr])
                                    systolic_err = tt.attr_error("Systolic", systolic, datapoint.data.systolic)
                                    diastolic_err = tt.attr_error("Diastolic", diastolic, datapoint.data.diastolic)
                                    assert systolic == datapoint.data.systolic and diastolic == datapoint.data.diastolic,\
                                        f'{error_preamble(row_nbr, csv_key)} {systolic_err} {diastolic_err}'
                                case dp.DataPointType.PULSE:
                                    value, uom = tt.decode_int(row[col_nbr])
                                    assert value == datapoint.data.value, \
                                        f'{error_preamble(row_nbr, csv_key)} {tt.attr_error("Pulse", value, datapoint.data.value)}'
                                case dp.DataPointType.BG:
                                    value, uom = tt.decode_int(row[col_nbr])
                                    assert value == datapoint.data.value, \
                                        f'{error_preamble(row_nbr, csv_key)} {tt.attr_error("Blood Glucose", value, datapoint.data.value)}'
                                case dp.DataPointType.BODY_WGT:
                                    value, uom = tt.decode_decimal(row[col_nbr])
                                    assert abs(value - datapoint.data.value) < 0.1, \
                                        f'{error_preamble(row_nbr, csv_key)} {tt.attr_error("Body Weight", value, datapoint.data.value)}'
                                case dp.DataPointType.BODY_TEMP:
                                    value, uom = tt.decode_decimal(row[col_nbr])
                                    assert abs(value - datapoint.data.value) < 0.1, \
                                        f'{error_preamble(row_nbr, csv_key)} {tt.attr_error("Body Temp", value, datapoint.data.value)}'
        return

    def retrieve_datapoints(self, msg: msgs.ImportExportCompletionMsg):
        """
        A callback method invoked when an ImportExportCompletionMsg is received

        :param msg: the Completion message
        :type msg: biometrics_tracker.ipc.mesages.ImporExportCompletionMsg
        :return: None

        """
        for line in msg.messages:
            self.logger.info(line)

        self.queue_mgr.send_db_req_msg(msgs.DataPointReqMsg(destination=per.DataBase, replyto=self.check_import,
                                                            person_id=self.person.id, start=self.taken_lower,
                                                            end=self.taken_upper,
                                                            operation=msgs.DBOperation.RETRIEVE_SET))
        msg: Optional[msgs.DataPointRespMsg] = None
        while msg is None:
            self.logger.debug(f'Checking database response queue for ID: {self.person.id}')
            msg = self.queue_mgr.check_db_resp_queue(block=True)
            if msg is not None and msg.destination is not None:
                self.logger.info(f'DataPoint Response Message received for ID: {self.person.id}')
                msg.destination(msg)
            else:
                sleep(5)

    def test_importer(self, person: dp.Person, datapoints_fix):
        """
        Test the CSVImporter class using the specified Person and DataPoints

        :param person: the Person object associated with the DataPoints
        :type person: biometrics_tracker.model.datapoints.Person
        :param datapoints_fix: a fixture providing a list of DataPoint instances
        :type datapoints_fix: collections.NamedTuple
        :return: None

        """
        self.csv_filepath, self.taken_lower, self.taken_upper = self.create_csv(person, datapoints_fix)
        importer: imp.CSVImporter = imp.CSVImporter(self.queue_mgr, self.person, self.csv_filepath.__str__(),
                                                    self.nbr_of_header_lines, self.use_prev_row_date,
                                                    self.indexed_fields, self.retrieve_datapoints)
        self.check_import_init(importer, person, self.retrieve_datapoints)
        importer.start()
        msg: Optional[msgs.ImportExportCompletionMsg] = None
        while msg is None:
            self.logger.debug(f'Checking completion queue for ID: {self.person.id}')
            msg = self.queue_mgr.check_completion_queue(block=True)
            if msg is not None and msg.destination is not None:
                self.logger.info(f'Completion Message received for ID: {self.person.id}')
                msg.destination(msg)
            else:
                sleep(5)


@pytest.mark.DataPoint
def test_csv_importer(tmpdir, people_fix, datapoints_fix):
    """
    Test the CSVImpporter class for a number of people.  This is implemented as a module level function because p
    ytest,
    or Pycharm's pytest framework doesn't seem to be able to properly run tests implemented as bound methods.

    :param tmpdir: the pytest tmpdir fixture - provides a temporary directory for test output files and logs
    :type tmpdir: pytest.Fixture
    :param people_fix: a fixture that provides a set of Person instances
    :type people_fix: list[biometrics_tracker.model.datapoints.Person]
    :param datapoints_fix: a fixture that provides a set of DataPoint instances associated with contents of people_fix
    :type datapoints_fix: collections.NamedTuple
    :return: None

    """
    temp_dir: pathlib.Path = pathlib.Path(tmpdir)
    for person in people_fix:
        test_importer: TestCSVImporter = TestCSVImporter(temp_dir, person, datapoints_fix)
        test_importer.test_importer(person, datapoints_fix)



