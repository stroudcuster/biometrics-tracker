from datetime import date, time, datetime, timedelta
import os
import pathlib
import pytest
from typing import Any, Callable, Optional

from datapoints_fixtures import person_fix, people_fix, tracking_config_fix, schedule_fix, datapoints_fix, \
    blood_pressure_dp_data_fix, pulse_dp_data_fix, blood_glucose_dp_data_fix, body_temp_dp_data_fix, \
    body_weight_dp_data_fix
import datapoints_fixtures as dp_fix

import persistence_fixtures as perfix

import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.persistence as per
from biometrics_tracker.utilities.utilities import max_dom

import tests.model.random_data as rd
from tests.test_tools import DATE_FMT, DATETIME_FMT, TIME_FMT, attr_error, compare_hms, compare_mdyhms, compare_object


@pytest.mark.Database
class TestPersistence:
    def __init__(self, temp_dir: pathlib.Path):
        self.temp_dir = temp_dir
        self.queue_mgr = queues.Queues(sleep_seconds=2)
        self.queue_mgr_patch = perfix.QueueManagerPatch()
        self.random_data = rd.BiometricsRandomData()
        self.db_path = pathlib.Path(self.temp_dir, 'biometrics.db')
        self.db = per.DataBase(self.db_path.__str__(), self.queue_mgr, block_req_queue=True)
        self.tbl_idx_list = ['PEOPLE', 'TRACK_CFG', 'SCHEDULE', 'DATAPOINTS', 'DP_IDX', 'TRACK_CFG_IDX',
                             'SCHEDULE_IDX']
        self.create_db_connection()
        self.now: datetime = datetime.now()

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

    def check_sqlite3(self) -> str:
        script: str = f'.open {self.db_path.__str__()}\n.table\n.index'
        script_path = pathlib.Path(self.temp_dir, 'script.txt')
        with script_path.open(mode='w') as sp:
            sp.write(script)
        script_resp: str = os.popen(f'sqlite3 < {script_path.__str__()}').read()
        return script_resp

    def test_create_database(self):
        self.db_open_do_close(do_action=self.db.create_db)
        script_resp = self.check_sqlite3()
        for item in self.tbl_idx_list:
            assert script_resp.find(item) >= 0, f'Table or Index {item} missing from database'

    def test_drop_database(self):
        self.db_open_do_close(do_action=self.db.drop_db)
        script_resp = self.check_sqlite3()
        for item in self.tbl_idx_list:
            assert script_resp.find(item) < 0, f'Table or Index {item} not dropped from database'

    @pytest.mark.Person
    def test_person_insert_retrieve(self, people: list[dp.Person]):
        person_map: dict[str, dp.Person] = {}

        def insert_people():
            self.db.create_db()
            for person in people:
                self.db.insert_person(person)
                person_map[person.id] = person

        def retrieve_people():
            for inserted in person_map.values():
                person = self.db.retrieve_person(inserted.id)
                if not compare_object(inserted, person):
                    assert inserted.id == person.id, \
                        f'Person ID: {inserted.id} {attr_error("ID", inserted.id, person.id)}'
                    assert inserted.name == person.name, \
                        f'Person ID: {inserted.id} {attr_error("Name", inserted.name, person.name)}'
                    assert inserted.dob == person.dob, \
                        f'Person ID: {inserted.id} {attr_error("D.O.B", inserted.dob.strftime(DATE_FMT), person.dob.strftime(DATE_FMT))}'
                    assert inserted.age == person.age, \
                        f'Person ID: {inserted.id} {attr_error("Age", inserted.age, person.age)}'

        self.db_open_do_close(do_action=insert_people)
        self.db_open_do_close(do_action=retrieve_people)

    def test_person_update(self, people: list[dp.Person]):
        person_map: dict[str, dp.Person] = {}

        def insert_people():
            self.db.create_db()
            for person in people:
                self.db.insert_person(person)
                person_map[person.id] = person

        def update_people():
            for person in person_map.values():
                person.name = self.random_data.random_string(length=20)
                person.dob = self.random_data.random_dob()
                self.db.update_person(person)
                check_person = self.db.retrieve_person(person.id)
                assert check_person is not None, f'Person ID: {person.id} retrieval failed'
                assert check_person.name == person.name, \
                    f'Person ID: {person.id} Update Error {attr_error("Name", person.name, check_person.name)}'
                expected: str = person.dob.strftime(DATE_FMT)
                observed:str = check_person.dob.strftime(DATE_FMT)
                assert check_person.dob == person.dob, \
                    f'Person ID: {person.id} Update Error {attr_error("D.O.B.", expected, observed)}'

        self.db_open_do_close(do_action=insert_people)
        self.db_open_do_close(do_action=update_people)

    def test_people_list(self, people: list[dp.Person]):
        id_name_list: list[(str, str)] = []
        people_list: list[tuple[str, str]] = []

        def insert_people():
            self.db.create_db()
            for person in people:
                self.db.insert_person(person)
                id_name_list.append((person.id, person.name))

        self.db_open_do_close(do_action=insert_people)
        people_list = self.db_open_do_close(do_action=self.db.retrieve_people)
        for id_name in id_name_list:
            assert id_name in people_list, f'Person ID: {id_name[0]} Name: {id_name[1]} not in People List'

    def test_tracking_cfg_insert(self, people_fix, tracking_config_fix):
        def insert():
            for person in people_fix:
                for track_cfg in tracking_config_fix:
                    person.tracked[track_cfg.dp_type] = track_cfg
                self.db.insert_person(person)

        def retrieve():
            for person in people_fix:
                check_person = self.db.retrieve_person(person.id)
                for track_cfg in person.tracked.values():
                    check_track_cfg = check_person.dp_type_track_cfg(track_cfg.dp_type)
                    assert check_track_cfg is not None, f'Tracking Config DPType: {track_cfg.dp_type} insert failed.'
                    expected: str = track_cfg.default_uom.name
                    observed: str = check_track_cfg.default_uom.name
                    assert check_track_cfg.default_uom == track_cfg.default_uom, \
                        f'Tracking Config DPType: {track_cfg.dp_type} {attr_error("UOM", expected, observed)}'

        self.db_open_do_close(insert)
        self.db_open_do_close(retrieve)

    def test_tracking_cfg_update(self, people_fix, tracking_config_fix):
        def insert():
            for person in people_fix:
                for track_cfg in tracking_config_fix:
                    person.tracked[track_cfg.dp_type] = track_cfg
                self.db.insert_person(person)

        def update():
            for person in people_fix:
                dp_types: list[dp.DataPointType] = person.tracked.keys()
                for dp_type in dp_types:
                    track_cfg = person.tracked[dp_type]
                    if dp_type.value == len(dp.DataPointType):
                        track_cfg.dp_type = dp.DataPointType(1)
                    else:
                        track_cfg.dp_type = dp.DataPointType(track_cfg.value+1)
                    del person.tracked[dp_type]
                    person.tracked[track_cfg.dp_type] = track_cfg
                    self.db.update_person(person)

        def retrieve():
            for person in people_fix:
                check_person = self.db.retrieve_person(person.id)
                for track_cfg in person.tracked.values():
                    check_track_cfg = check_person.dp_type_track_cfg(track_cfg.dp_type)
                    assert check_track_cfg is not None, f'Tracking Config DPType: {track_cfg.dp_type} insert failed.'
                    expected: str = track_cfg.default_uom.name
                    observed: str = check_track_cfg.default_uom.name
                    assert check_track_cfg.default_uom == track_cfg.default_uom, \
                        f'Tracking Config DPType: {track_cfg.dp_type} {attr_error("UOM", expected, observed)}'

        self.db_open_do_close(insert)
        self.db_open_do_close(update)
        self.db_open_do_close(retrieve)

    def sched_err_preamble(self, sched: dp.ScheduleEntry):
        return f'Schedule Person ID: {sched.person_id} Seq Nbr: {sched.seq_nbr:3d} '

    def insert_schedules(self, schedule_fix):
        for schedule in schedule_fix:
            self.db.insert_schedule_entry(schedule)

    def test_schedule_insert(self, schedule_fix):
        def retrieve():
            person_id: str = schedule_fix[0].person_id
            schedules: list[dp.ScheduleEntry] = self.db.retrieve_schedules_for_person(person_id)
            sched_map: dict[int, dp.ScheduleEntry] = {schedule.seq_nbr: schedule for schedule in schedules}
            for schedule in schedule_fix:
                assert schedule.seq_nbr in sched_map, f'{self.sched_err_preamble(schedule)} not inserted.'
                check_sched = sched_map[schedule.seq_nbr]
                if not compare_object(schedule, check_sched):
                    assert schedule.person_id == check_sched.person_id, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Person ID", schedule.person_id, check_sched.person_id)}'
                    assert schedule.seq_nbr == check_sched.seq_nbr, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Seq Nbr", schedule.seq_nbr, check_sched.seq_nbr)}'
                    assert schedule.dp_type == check_sched.dp_type, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("DP Type", schedule.dp_type.name, check_sched.dp_type.name)}'
                    starts: str = schedule.starts_on.strftime(DATE_FMT)
                    chk_starts: str = check_sched.starts_on.strftime(DATE_FMT)
                    assert schedule.starts_on == check_sched.starts_on, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Starts On", starts, chk_starts)}'
                    ends: str = schedule.ends_on.strftime(DATE_FMT)
                    chk_ends: str = check_sched.ends_on.strftime(DATE_FMT)
                    assert schedule.ends_on == check_sched.ends_on, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Ends On", ends, chk_ends)}'
                    freq_name: str = schedule.frequency.name
                    chk_freq_name: str = check_sched.frequency.name
                    assert schedule.frequency == check_sched.frequency, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Frequency", freq_name, chk_freq_name)}'
                    assert schedule.interval == check_sched.interval, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Interval", schedule.interval, check_sched.interval)}'
                    when_time = schedule.when_time.strftime(TIME_FMT)
                    chk_when_time = check_sched.when_time.strftime(TIME_FMT)
                    assert compare_hms(schedule.when_time, check_sched.when_time), \
                        f'{self.sched_err_preamble(schedule)} {attr_error("When Time", when_time, chk_when_time)}'
                    wkdays = ','.join(weekday.name for weekday in schedule.weekdays)
                    chk_wkdays = ','.join(weekday.name for weekday in check_sched.weekdays)
                    assert schedule.weekdays == check_sched.weekdays, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Week Days", wkdays, chk_wkdays)}'
                    doms = ','.join(str(dom) for dom in schedule.days_of_month)
                    chk_doms = ','.join(str(dom) for dom in check_sched.days_of_month)
                    assert schedule.days_of_month == check_sched.days_of_month, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Days of Month", doms, chk_doms)}'
                    assert schedule.suspended == check_sched.suspended, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Suspended", str(schedule.suspended), str(check_sched.suspended))}'
                    last_trig = schedule.last_triggered.strftime(DATETIME_FMT)
                    chk_last_trig = check_sched.last_triggered.strftime(DATETIME_FMT)
                    assert compare_mdyhms(schedule.last_triggered, check_sched.last_triggered), \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Last Triggered", last_trig, chk_last_trig)}'

            self.db_open_do_close(lambda s=schedule_fix: self.insert_schedules(s))
            self.db_open_do_close(retrieve)

    def test_schedule_update(self, schedule_fix):
        def update():
            for schedule in schedule_fix:
                lower: date = datetime.now().date() - timedelta(days=30)
                upper: date = datetime.now().date() + timedelta(days=45)
                schedule.starts_on, schedule.ends_on = self.random_data.random_date_pair(lower, upper)
                if schedule.frequency.value == len(dp.FrequencyType):
                    schedule.frequency = dp.FrequencyType(1)
                else:
                    schedule.frequency = dp.FrequencyType(schedule.frequency.value+1)
                schedule.when_time = schedule.when_time + timedelta(hours=2)
                if len(schedule.weekdays) > 0:
                    for day in dp.WeekDay:
                        if day not in schedule.weekdays:
                            schedule.weekdays.append(day)
                            break
                if len(schedule.days_of_month) > 0:
                    for day in range(1, max_dom(schedule.starts_on.month)+1):
                        if day not in schedule.days_of_month:
                            schedule.days_of_month.append(day)
                            break
                schedule.interval = self.random_data.random_int(1, 4)
                self.db.update_schedule_entry(schedule)

        def retrieve():
            person_id: str = schedule_fix[0].person_id
            schedules: list[dp.ScheduleEntry] = self.db.retrieve_schedules_for_person(person_id)
            sched_map: dict[int, dp.ScheduleEntry] = {schedule.seq_nbr: schedule for schedule in schedules}
            for schedule in schedule_fix:
                assert schedule.seq_nbr in sched_map, f'{self.sched_err_preamble(schedule)} not inserted.'
                check_sched = sched_map[schedule.seq_nbr]
                if not compare_object(schedule, check_sched):
                    assert schedule.dp_type == check_sched.dp_type, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("DP Type", schedule.dp_type.name, check_sched.dp_type.name)}'
                    starts: str = schedule.starts_on.strftime(DATE_FMT)
                    chk_starts: str = check_sched.starts_on.strftime(DATE_FMT)
                    assert schedule.starts_on == check_sched.starts_on, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Starts On", starts, chk_starts)}'
                    ends: str = schedule.ends_on.strftime(DATE_FMT)
                    chk_ends: str = check_sched.ends_on.strftime(DATE_FMT)
                    assert schedule.ends_on == check_sched.ends_on, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Ends On", ends, chk_ends)}'
                    freq_name: str = schedule.frequency.name
                    chk_freq_name: str = check_sched.frequency.name
                    assert schedule.frequency == check_sched.frequency, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Frequency", freq_name, chk_freq_name)}'
                    assert schedule.interval == check_sched.interval, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Interval", schedule.interval, check_sched.interval)}'
                    when_time = schedule.when_time.strftime(TIME_FMT)
                    chk_when_time = check_sched.when_time.strftime(TIME_FMT)
                    assert compare_hms(schedule.when_time, check_sched.when_time), \
                        f'{self.sched_err_preamble(schedule)} {attr_error("When Time", when_time, chk_when_time)}'
                    wkdays = ','.join(weekday.name for weekday in schedule.weekdays)
                    chk_wkdays = ','.join(weekday.name for weekday in check_sched.weekdays)
                    assert schedule.weekdays == check_sched.weekdays, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Week Days", wkdays, chk_wkdays)}'
                    doms = ','.join(str(dom) for dom in schedule.days_of_month)
                    chk_doms = ','.join(str(dom) for dom in check_sched.days_of_month)
                    assert schedule.days_of_month == check_sched.days_of_month, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Days of Month", doms, chk_doms)}'
                    assert schedule.suspended == check_sched.suspended, \
                        f'{self.sched_err_preamble(schedule)} {attr_error("Suspended", str(schedule.suspended), str(check_sched.suspended))}'

            self.db_open_do_close(lambda s=schedule_fix: self.insert_schedules(s))
            self.db_open_do_close(update)
            self.db_open_do_close(retrieve)

    def test_schedule_update_last_triggered(self, schedule_fix):
        def update():
            self.now = datetime.now()
            for schedule in schedule_fix:
                self.db.update_schedule_last_triggered(schedule.person_id, schedule.seq_nbr, self.now)

        def retrieve():
            person_id: str = schedule_fix[0].person_id
            schedules: list[dp.ScheduleEntry] = self.db.retrieve_schedules_for_person(person_id)
            schedules = self.db.retrieve_schedules_for_person(person_id)
            exp: str = self.now.strftime(DATETIME_FMT)
            for schedule in schedules:
                obs: str = schedule.last_triggered.strftime(DATETIME_FMT)
                assert compare_mdyhms(schedule.last_triggered, self.now), \
                    f'{self.sched_err_preamble(schedule)} {attr_error("Last Triggered", exp, obs)}'

        self.db_open_do_close(lambda s=schedule_fix: self.insert_schedules(s))
        self.db_open_do_close(update)
        self.db_open_do_close(retrieve)

    def test_schedule_delete(self, schedule_fix):
        def delete():
            single_delete_count: int = 0
            for schedule in schedule_fix:
                self.db.delete_schedule_for_person_seq(schedule.person_id, schedule.seq_nbr)
                check_sched: list[dp.ScheduleEntry] = self.db.retrieve_schedules_for_person(schedule.person_id)
                seq_nbrs: list[int] = [sched.seq_nbr for sched in check_sched]
                assert schedule.seq_nbr not in seq_nbrs, f'{self.sched_err_preamble(schedule)} not deleted person/seq del'
                single_delete_count += 1
                if single_delete_count >= 2:
                    break
            person_id = schedule_fix[0].person_id
            self.db.delete_schedules_for_person(person_id)
            check_sched: list[dp.ScheduleEntry] = self.db.retrieve_schedules_for_person(person_id)
            assert len(check_sched) == 0, \
                f'{self.sched_err_preamble(schedule_fix[0])} not deleted person del'

        self.db_open_do_close(lambda s=schedule_fix: self.insert_schedules(s))
        self.db_open_do_close(delete)

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

    def dp_err_preamble(self, datap: dp.DataPoint):
        return f'DataPoint Person ID: {datap.person_id} Taken: {datap.taken.strftime(DATETIME_FMT)} Type: {datap.type.name}'

    @pytest.mark.DataBase
    @pytest.mark.DataPoint
    def test_datapoint_insert(self, people_fix, datapoints_fix):
        def make_key(datapoint: dp.DataPoint) -> str:
            return f'{datapoint.person_id}:{datapoint.taken.strftime(DATETIME_FMT)}:{datapoint.type.name}'

        def retrieve_datapoints():
            for person in people_fix:
                check_dps: list[dp.DataPoint] = self.db.retrieve_datapoints_for_person(person.id, first_taken,
                                                                                       last_taken)
                for chk_dp in check_dps:
                    key = make_key(chk_dp)
                    if key in datapoints_map:
                        datapoint = datapoints_map[key]
                        if not compare_object(datapoint, chk_dp):
                            assert datapoint.note == chk_dp.note,\
                                f'{self.dp_err_preamble(datapoint)} {attr_error("Note", datapoint.note, chk_dp.note)}'
                            assert compare_object(datapoint.data, chk_dp.data), \
                                f'{self.dp_err_preamble(datapoint)} {attr_error("Data", datapoint.data, chk_dp.data)}'
                    else:
                        assert False, f'{self.dp_err_preamble(chk_dp)} Person ID, Taken or DP Type mismatch'

        self.create_db_connection()
        datapoints_map, first_taken, last_taken = self.insert_datapoints(datapoints_fix)
        self.close_db_connection()
        self.db_open_do_close(retrieve_datapoints)


@pytest.mark.DataBase
def test_create_drop(tmpdir):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_create_database()
    per_tests.test_drop_database()


@pytest.mark.Database
@pytest.mark.Person
def test_person_insert_retrieve(tmpdir, people_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_person_insert_retrieve(people_fix)


@pytest.mark.Database
@pytest.mark.Person
def test_person_update(tmpdir, people_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_person_update(people_fix)


@pytest.mark.Database
@pytest.mark.Person
def test_people_list(tmpdir, people_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_people_list(people_fix)


@pytest.mark.Database
@pytest.mark.Person
@pytest.mark.TrackingConfig
def test_track_cfg_insert(tmpdir, people_fix, tracking_config_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_tracking_cfg_insert(people_fix, tracking_config_fix)


@pytest.mark.Database
@pytest.mark.Person
@pytest.mark.TrackingConfig
def test_track_cfg_update(tmpdir, people_fix, tracking_config_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_tracking_cfg_insert(people_fix, tracking_config_fix)


@pytest.mark.Database
@pytest.mark.Person
@pytest.mark.Schedule
def test_schedule_insert(tmpdir, schedule_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_schedule_insert(schedule_fix)


@pytest.mark.Database
@pytest.mark.Person
@pytest.mark.Schedule
def test_schedule_update(tmpdir, schedule_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_schedule_update(schedule_fix)


@pytest.mark.Database
@pytest.mark.Person
@pytest.mark.Schedule
def test_schedule_update_last_triggered(tmpdir, schedule_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_schedule_update_last_triggered(schedule_fix)


@pytest.mark.Database
@pytest.mark.Person
@pytest.mark.Schedule
def test_schedule_delete(tmpdir, schedule_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_schedule_delete(schedule_fix)


@pytest.mark.Database
@pytest.mark.DataPoint
def test_datapoint_insert(tmpdir, people_fix, datapoints_fix):
    per_tests = TestPersistence(tmpdir)
    per_tests.test_datapoint_insert(people_fix, datapoints_fix)

