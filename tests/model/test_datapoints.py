from datetime import date, time, datetime, timedelta
import pytest
from typing import Any

from tests.model.datapoints_fixtures import person_data_fix, person_fix, tracking_config_fix, schedule_fix, \
    blood_pressure_data_fix, blood_glucose_data_fix, pulse_data_fix, body_weight_data_fix, body_temp_data_fix, \
    blood_glucose_dp_data_fix, blood_pressure_dp_data_fix, pulse_dp_data_fix, body_temp_dp_data_fix, \
    body_weight_dp_data_fix


import biometrics_tracker.model.datapoints as dp

from tests.model.random_data import BiometricsRandomData
from tests.test_tools import DATETIME_FMT, DATE_FMT, TIME_FMT, attr_error, compare_hms, compare_mdyhms, compare_object


random_data: BiometricsRandomData = BiometricsRandomData()


@pytest.mark.Person
def test_person_init(person_data_fix):
    """
    Test biometrics_tracker.model.datapoints.Person initialization

    :param person_data_fix: a fixture providing data to create Person instances
    :type person_data_fix: collections.namedtuple('person_data_t', ['id', 'name', 'dob', 'age'])
    :return: None

    """
    person: dp.Person = dp.Person(id=person_data_fix.id,
                                  name=person_data_fix.name,
                                  dob=person_data_fix.dob)
    assert person is not None, 'Person object not created.'
    assert person_data_fix.id == person.id, attr_error('Person ID', person_data_fix.id, person.id)
    assert person_data_fix.name == person.name, attr_error('Person Name', person_data_fix.name, person.name)
    assert person_data_fix.dob == person.dob, attr_error('Person D.O.B', person_data_fix.dob.strftime('%m/%d/%Y'),
                                                         person.dob.strftime('%m/%d/%Y'))


@pytest.mark.TrackingConfig
def test_tracking_config_init(tracking_config_fix):
    """
    Test biometrics_tracker.model.datapoints.TrackingConfig initialization

    :param tracking_config_fix: a fixture providing a list of TrackingConfig instances
    :type tracking_config_fix: list[biometrics_tracker.model.datapoints.TrackingConfig]
    :return: None

    """
    track_cfg = dp.TrackingConfig(dp_type=tracking_config_fix[0].dp_type,
                                  default_uom=tracking_config_fix[0].default_uom,
                                  tracked=tracking_config_fix[0].tracked)
    assert track_cfg is not None, 'Tracking Config object not created'
    assert track_cfg.dp_type == tracking_config_fix[0].dp_type, attr_error('Tracking Config DP Type',
                                                                           tracking_config_fix[0].dp_type.name,
                                                                           track_cfg.dp_type.name)
    assert track_cfg.default_uom == tracking_config_fix[0].default_uom, attr_error('Tracking Config UOM',
                                                                           tracking_config_fix[0].default_uom.name,
                                                                           track_cfg.default_uom.name)
    assert track_cfg.tracked == tracking_config_fix[0].tracked, attr_error('Tracking Config Tracked',
                                                                           tracking_config_fix[0].tracked,
                                                                           track_cfg.tracked)


@pytest.mark.Person
@pytest.mark.TrackingConfig
def test_person_add_tracked_dp_type(person_fix, tracking_config_fix):
    """
    Tests the biometrics_tracker.model.datapoints.Person.add_tracked_dp_type method

    :param person_fix: a fixture providing Person instances
    :type person_fix: biometrics_tracker.model.datapoints.Person
    :param tracking_config_fix: a fixture providing TrackingConfig instances
    :type tracking_config_fix: list[biometrics_tracker.model.datapoints.TrackingConfig]
    :return: None

    """
    for datum in tracking_config_fix:
        person_fix.add_tracked_dp_type(datum.dp_type, dp.TrackingConfig(datum.dp_type, datum.default_uom, datum.tracked))
    assert len(person_fix.tracked) == len(tracking_config_fix), attr_error('Person TrackingConfig List Len',
                                                                           len(person_fix.tracked),
                                                                           len(tracking_config_fix))
    for datum in tracking_config_fix:
        assert datum.dp_type in person_fix.tracked, f'No Tracking Config for DataPointType {datum.dp_type.name}'
        assert datum.default_uom == person_fix.tracked[datum.dp_type].default_uom, attr_error(
            f'Tracking Config for {datum.dp_type.name} Default UOM', datum.default_uom.name,
            person_fix.tracked[datum.dp_type].default_uom.name)
        assert datum.tracked == person_fix.tracked[datum.dp_type].tracked, attr_error(
            f'Tracking Config for {datum.dp_type}', datum.tracked, person_fix.tracked[datum.dp_type].tracked)


@pytest.mark.Person
@pytest.mark.TrackingConfig
def test_person_remove_tracked_dp_type(person_fix, tracking_config_fix):
    """
    Tests the biometrics_tracker.model.datapoints.Person.remove_tracked_dp_type method

    :param person_fix: a fixture providing Person instances
    :type person_fix: biometrics_tracker.model.datapoints.Person
    :param tracking_config_fix: a fixture providing TrackingConfig instances
    :type tracking_config_fix: list[biometrics_tracker.model.datapoints.TrackingConfig]
    :return: None

    """
    for datum in tracking_config_fix:
        person_fix.add_tracked_dp_type(datum.dp_type, dp.TrackingConfig(datum.dp_type, datum.default_uom, datum.tracked))
    assert len(person_fix.tracked) == len(tracking_config_fix), attr_error(
        'Person TrackingConfig List Len before remove', len(tracking_config_fix), len(person_fix.tracked))
    for idx, datum in enumerate(tracking_config_fix):
        rmv_dp_type = datum.dp_type
        person_fix.remove_tracked_dp_type(rmv_dp_type)
        expected_len: int = len(tracking_config_fix) - (idx + 1)
        assert len(person_fix.tracked) == expected_len, attr_error(
            'Person TrackingConfig List Len after remove', expected_len, len(person_fix.tracked))
        assert rmv_dp_type not in person_fix.tracked, f'Person Tracking Config {rmv_dp_type.name} not removed'


@pytest.mark.Person
@pytest.mark.TrackingConfig
def test_person_is_tracked(person_fix, tracking_config_fix):
    """
    Tests the biometrics_tracker.model.datapoints.Person.is_tracked method

    :param person_fix: a fixture providing Person instances
    :type person_fix: biometrics_tracker.model.datapoints.Person
    :param tracking_config_fix: a fixture providing TrackingConfig instances
    :type tracking_config_fix: list[biometrics_tracker.model.datapoints.TrackingConfig]
    :return: None

    """
    for datum in tracking_config_fix:
        person_fix.add_tracked_dp_type(datum.dp_type, dp.TrackingConfig(datum.dp_type, datum.default_uom,
                                                                        datum.tracked))
    assert len(person_fix.tracked) == len(tracking_config_fix), attr_error('Person TrackingConfig List Len',
                                                                           len(tracking_config_fix),
                                                                           len(person_fix.tracked))
    for datum in tracking_config_fix:
        assert datum.tracked == person_fix.is_tracked(datum.dp_type), attr_error(
            f'Person TrackingConfig {datum.dp_type.name} Tracked', datum.tracked, person_fix.is_tracked(datum.dp_type))


@pytest.mark.Person
@pytest.mark.TrackingConfig
def test_person_dp_type_track_config(person_fix, tracking_config_fix):
    """
    Tests the biometrics_tracker.model.datapoints.Person.dp_type_track_cfg method

    :param person_fix: a fixture providing Person instances
    :type person_fix: biometrics_tracker.model.datapoints.Person
    :param tracking_config_fix: a fixture providing TrackingConfig instances
    :type tracking_config_fix: list[biometrics_tracker.model.datapoints.TrackingConfig]
    :return: None

    """
    for datum in tracking_config_fix:
        tc1: dp.TrackingConfig = dp.TrackingConfig(datum.dp_type, datum.default_uom, datum.tracked)
        person_fix.add_tracked_dp_type(datum.dp_type, tc1)
        tc2 = person_fix.dp_type_track_cfg(datum.dp_type)
        assert tc1.dp_type == tc2.dp_type, attr_error('Tracking Config DP Type', tc1.dp_type.name, tc2.dp_type.name)
        assert tc1.default_uom == tc2.default_uom, attr_error(f'Tracking Config {tc1.dp_type.name} UOM',
                                                              tc1.default_uom.name, tc2.default_uom.name)
        assert tc1.tracked == tc2.tracked, attr_error(f'Tracking Config {tc1.dp_type} Tracked', tc1.tracked,
                                                      tc2.tracked)


@pytest.mark.Schedule
def test_frequency_name_map():
    """
    Tests the biometrics_tracker.model.datapoints.frequency_name map

    :return: None

    """
    for freq in dp.FrequencyType:
        assert freq.name in dp.frequency_name_map, f'Frequency {freq.name} not in frequency_name_map'
        assert freq == dp.frequency_name_map[freq.name], attr_error(f'frequency_name_map for Frequency {freq.name}',
                                                                    freq.name, dp.frequency_name_map[freq.name])
    assert len(dp.FrequencyType) == len(dp.frequency_name_map), attr_error('frequency_name_map length',
                                                                           len(dp.FrequencyType),
                                                                           len(dp.frequency_name_map))


@pytest.mark.Schedule
def test_weekday_name_map():
    """
    Tests the biometrics_tracker.model.datapoints.weekday_name_map

    :return: None

    """
    for day in dp.WeekDay:
        assert day.name in dp.weekday_name_map, f'WeekDay {day.name} not in weekday_name_map'
        assert day == dp.weekday_name_map[day.name], attr_error(f'weekday_name_map for WeekDay {day.name}',
                                                                day.name, dp.weekday_name_map[day.name])
    assert len(dp.WeekDay) == len(dp.weekday_name_map), attr_error('weekday_name_maP length',
                                                                   len(dp.WeekDay), len(dp.weekday_name_map))


@pytest.mark.Schedule
def test_schedule_init(schedule_fix):
    """
    Tests biometrics_tracker.model.datapoints.ScheduleEntry initialization

    :param schedule_fix: a fixture providing a list of biometrics_tracker.model.datapoints.ScheduleEntry objects
    :type schedule_fix: list[biometrics_tracker.model.datapoints.ScheduleEntry]
    :return: None

    """
    for datum in schedule_fix:
        schedule = dp.ScheduleEntry(datum.person_id, datum.seq_nbr, datum.frequency, datum.dp_type, datum.note,
                                    datum.weekdays, datum.days_of_month, datum.interval, datum.when_time,
                                    datum.starts_on, datum.ends_on, datum.suspended, datum.last_triggered)
        assert schedule.person_id == datum.person_id, attr_error(f'Schedule {datum.frequency.name} Person ID',
                                                                 datum.person_id, schedule.person_id)
        assert schedule.seq_nbr == datum.seq_nbr, attr_error(f'Schedule {datum.frequency.name} Seq Nbr',
                                                             datum.seq_nbr, schedule.seq_nbr)
        assert schedule.dp_type == datum.dp_type, attr_error(f'Schedule {datum.frequency.name} DP Type', datum.dp_type,
                                                             schedule.dp_type)
        assert schedule.note == datum.note, attr_error(f'Schedule {datum.frequency.name} Note', datum.note,
                                                       schedule.note)
        schedule_days_str = ''
        for day in schedule.weekdays:
            schedule_days_str = f'{schedule_days_str} [{day.name}]'
        datum_days_str = ''
        for day in datum.weekdays:
            datum_days_str = f'{datum_days_str} [{day.name}]'
        assert schedule.weekdays == datum.weekdays, attr_error(f'Schedule {datum.frequency.name} WeekDays ',
                                                               datum_days_str, schedule_days_str)
        schedule_dom_str = ''
        for dom in schedule.days_of_month:
            schedule_dom_str = f'{schedule_dom_str} [{dom:d}]'
        datum_dom_str = ''
        for dom in datum.days_of_month:
            datum_dom_str = f'{datum_dom_str} [{dom:d}'
        assert schedule.days_of_month == datum.days_of_month, attr_error(
            f'Schedule {datum.frequency.name} Days of Month', datum_dom_str, schedule_dom_str)

        assert schedule.interval == datum.interval, attr_error(f'Schedule {datum.frequency.name}',
                                                               str(datum.interval), str(schedule.interval))
        assert schedule.when_time == datum.when_time, attr_error(f'Schedule {datum.frequency.name} When Time',
                                                                 datum.when_time.strftime(TIME_FMT),
                                                                 schedule.when_time.strftime(TIME_FMT))
        assert schedule.starts_on == datum.starts_on, attr_error(f'Schedule {datum.frequency.name} Starts On',
                                                                 datum.starts_on.strftime(DATE_FMT),
                                                                 schedule.starts_on.strftime(DATE_FMT))
        assert schedule.ends_on == datum.ends_on, attr_error(f'Schedule {datum.frequency.name} Ends On',
                                                             datum.ends_on.strftime(DATE_FMT),
                                                             schedule.ends_on.strftime(DATE_FMT))
        assert schedule.suspended == datum.suspended, attr_error(f'Schedule {datum.frequency.name} Suspended',
                                                                 datum.suspended, schedule.suspended)
        assert schedule.last_triggered == datum.last_triggered, attr_error(
            f'Schedule {datum.frequency.name} Last Triggered', datum.last_triggered.strftime(DATETIME_FMT),
            schedule.last_triggered.strftime(DATETIME_FMT))


@pytest.mark.Schedule
def test_schedule_get_weekday_dates(schedule_fix):
    """
    Tests biometrics_tracker.model.datapoints.ScheduleEntry.get_weekday_dates method

    :param schedule_fix: a fixture providing a list of biometrics_tracker.model.datapoints.ScheduleEntry objects
    :type schedule_fix: list[biometrics_tracker.model.datapoints.ScheduleEntry]
    :return: None

    """
    datum = schedule_fix[0]
    schedule = dp.ScheduleEntry(datum.person_id, datum.seq_nbr, datum.frequency, datum.dp_type, datum.note,
                                datum.weekdays, datum.days_of_month, datum.interval, datum.when_time,
                                datum.starts_on, datum.ends_on, datum.suspended, datum.last_triggered)
    for day in dp.WeekDay:
        day_dates: list[date] = schedule.get_weekday_dates(day)
        for day_date in day_dates:
            assert day_date.weekday()+1 == day.value, attr_error(f'{schedule.__class__.__name__}.get_weekday_dates: '
                                                                 f'Date: {day_date.strftime(DATE_FMT)}', day.value,
                                                                 day_date.weekday()+1)


@pytest.mark.Schedule
def test_schedule_next_occurrence_today(schedule_fix):
    """
    Tests biometrics_tracker.model.datapoints.ScheduleEntry.next_occurrence_today method

    :param schedule_fix: a fixture providing a list of biometrics_tracker.model.datapoints.ScheduleEntry objects
    :type schedule_fix: list[biometrics_tracker.model.datapoints.ScheduleEntry]
    :return: None

    """

    def error_preamble() -> str:
        return f'ScheduleEntry.next_occurrence_today Frequency {schedule.frequency.name} ' \
               f'Starts On {schedule.starts_on.strftime(DATE_FMT)} Ends On {schedule.ends_on.strftime(DATE_FMT)} ' \
               f'Interval {schedule.interval} When : {schedule.when_time}'
    datum = schedule_fix[0]
    schedule = dp.ScheduleEntry(datum.person_id, datum.seq_nbr, datum.frequency, datum.dp_type, datum.note,
                                datum.weekdays, datum.days_of_month, datum.interval, datum.when_time,
                                datum.starts_on, datum.ends_on, datum.suspended, datum.last_triggered)
    # check One Time
    schedule.frequency = dp.FrequencyType.One_Time
    schedule.starts_on = date(year=date.today().year, month=date.today().month, day=date.today().day)
    when_datetime: datetime = datetime.now() + timedelta(hours=2)
    schedule.ends_on = date(year=when_datetime.year, month=when_datetime.month, day=when_datetime.day)
    schedule.when_time = when_datetime.time()
    schedule.interval = 1
    schedule.weekdays = []
    schedule.days_of_month = []
    schedule.suspended = False
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    assert len(next_occurrence) == 1, attr_error(f'{error_preamble()} No. of Occurrences', 1, len(next_occurrence))
    assert compare_hms(next_occurrence[0], schedule.when_time), attr_error(f'{error_preamble()} Occurrence Time',
                                                                           schedule.when_time.strftime(TIME_FMT),
                                                                           next_occurrence[0].strftime(TIME_FMT))

    schedule.suspended = True
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    assert next_occurrence is None or len(next_occurrence) == 0, attr_error(
        f'{error_preamble()} No. of Occurrences (Suspended)', 0, len(next_occurrence))

    # check Hourly
    schedule.frequency = dp.FrequencyType.Hourly
    schedule.starts_on = date(year=date.today().year, month=date.today().month, day=date.today().day)
    when_datetime: datetime = datetime.now()
    schedule.ends_on = date(year=when_datetime.year, month=when_datetime.month, day=when_datetime.day)
    schedule.when_time = when_datetime.time()
    schedule.interval = 2
    schedule.weekdays = []
    schedule.days_of_month = []
    schedule.suspended = False
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    expected_occ = (24 - schedule.when_time.hour) // schedule.interval
    assert abs(len(next_occurrence) - expected_occ) <= 1, attr_error(f'{error_preamble()} No. of Occurrences',
                                                           expected_occ, len(next_occurrence))
    for idx, occ in enumerate(next_occurrence):
        observed_interval: int = occ.hour - schedule.when_time.hour
        expected_interval: int = schedule.interval * idx
        assert observed_interval == expected_interval, attr_error(f'{error_preamble()} Hourly Interval',
                                                                  expected_interval, observed_interval)
    schedule.suspended = True
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    assert next_occurrence is None or len(next_occurrence) == 0, attr_error(
        f'{error_preamble()} No. of Occurrences (Suspended)', 0, len(next_occurrence))

    # check Daily
    schedule.frequency = dp.FrequencyType.Daily
    schedule.starts_on = date(year=date.today().year, month=date.today().month, day=date.today().day)
    when_datetime: datetime = datetime.now() + timedelta(hours=1)
    schedule.ends_on = date(year=when_datetime.year, month=when_datetime.month, day=when_datetime.day)
    schedule.when_time = when_datetime.time()
    schedule.interval = 1
    schedule.weekdays = []
    schedule.days_of_month = []
    schedule.suspended = False
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    assert len(next_occurrence) == 1, attr_error(f'{error_preamble()} No. of Occurrences', 1, len(next_occurrence))
    assert compare_hms(next_occurrence[0].time(), schedule.when_time), attr_error(
        f'{error_preamble()} Time', schedule.when_time.strftime(TIME_FMT), next_occurrence[0].strftime(TIME_FMT))

    schedule.suspended = True
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    assert next_occurrence is None or len(next_occurrence) == 0, attr_error(
        f'{error_preamble()} No. of Occurrences (Suspended)', 0, len(next_occurrence))

    # check Weekly
    schedule.frequency = dp.FrequencyType.Weekly
    starts_on = date.today() - timedelta(days=7)
    schedule.starts_on = date(year=starts_on.year, month=starts_on.month, day=starts_on.day)
    when_datetime: datetime = datetime.now() + timedelta(hours=1)
    ends_on: datetime = when_datetime + timedelta(days=7)
    schedule.ends_on = date(year=ends_on.year, month=ends_on.month, day=ends_on.day)
    schedule.when_time = when_datetime.time()
    schedule.interval = 1
    this_weekday: dp.WeekDay = dp.WeekDay(starts_on.weekday()+1)
    schedule.weekdays = random_data.random_weekdays(3)
    if this_weekday not in schedule.weekdays:
        schedule.weekdays.append(this_weekday)
    schedule.days_of_month = []
    schedule.suspended = False
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    assert len(next_occurrence) == 1, attr_error(f'{error_preamble()} No. of Occurrences', 1, len(next_occurrence))
    assert compare_hms(next_occurrence[0].time(), schedule.when_time), attr_error(
        f'{error_preamble()} Time', schedule.when_time.strftime(TIME_FMT), next_occurrence[0].strftime(TIME_FMT))

    schedule.suspended = True
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    assert next_occurrence is None or len(next_occurrence) == 0, attr_error(
        f'{error_preamble()} No. of Occurrences (Suspended)', 0, len(next_occurrence))

    # Check Monthly
    schedule.frequency = dp.FrequencyType.Monthly
    starts_on = date.today() - timedelta(days=7)
    schedule.starts_on = date(year=starts_on.year, month=starts_on.month, day=starts_on.day)
    when_datetime: datetime = datetime.now() + timedelta(hours=1)
    ends_on: datetime = when_datetime + timedelta(days=7)
    schedule.ends_on = date(year=ends_on.year, month=ends_on.month, day=ends_on.day)
    schedule.when_time = when_datetime.time()
    schedule.interval = 1
    schedule.weekdays = []
    schedule.days_of_month = random_data.random_dom(schedule.starts_on.month, 10)
    if schedule.starts_on.day not in schedule.days_of_month:
        schedule.days_of_month.append(schedule.starts_on.day)
    schedule.suspended = False
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    assert len(next_occurrence) == 1, attr_error(f'{error_preamble()} No. of Occurrences', 1, len(next_occurrence))
    assert compare_hms(next_occurrence[0].time(), schedule.when_time), attr_error(
        f'{error_preamble()} Time', schedule.when_time.strftime(TIME_FMT), next_occurrence[0].strftime(TIME_FMT))

    schedule.suspended = True
    next_occurrence: list[datetime] = schedule.next_occurrence_today(schedule.starts_on)
    assert next_occurrence is None or len(next_occurrence) == 0, attr_error(
        f'{error_preamble()} No. of Occurrences (Suspended)', 0, len(next_occurrence))


@pytest.mark.Person
@pytest.mark.Schedule
def test_person_add_schedule(person_fix, schedule_fix):
    """
    Tests biometrics_tracker.model.datapoints.Person.add_entry_sched method

    :param person_fix: a fixture providing a biometrics_tracking.model.datapoint.Person object
    :type person_fix: biometrics_tracking.model.datapoints.Person
    :param schedule_fix: a fixture providing a list of biometrics_tracker.model.datapoints.ScheduleEntry objects
    :type schedule_fix: list[biometrics_tracker.model.datapoints.ScheduleEntry]
    :return: None

    """

    def error_preamble() -> str:
        return f'Person.add_entry_sched Frequency {schedule.frequency.name} ' \
               f'Starts On {schedule.starts_on.strftime(DATE_FMT)} Ends On {schedule.ends_on.strftime(DATE_FMT)} ' \
               f'Interval {schedule.interval} When : {schedule.when_time}'

    for idx, datum in enumerate(schedule_fix):
        seq_nbr: int = idx + 1
        schedule = dp.ScheduleEntry(datum.person_id, seq_nbr, datum.frequency, datum.dp_type, datum.note,
                                    datum.weekdays, datum.days_of_month, datum.interval, datum.when_time,
                                    datum.starts_on, datum.ends_on, datum.suspended, datum.last_triggered)
        person_fix.add_entry_sched(schedule)
        assert len(person_fix.entry_schedules) == idx + 1, attr_error(f'{error_preamble()} Schedule Dict Size',
                                                                      idx + 1, len(person_fix.entry_schedules))
        assert seq_nbr in person_fix.entry_schedules, f'{error_preamble()} Schedule not posted to Person ' \
                                                      f'Schedule Dict'
        sched2 = person_fix.entry_schedules[seq_nbr]
        assert compare_object(schedule, sched2), attr_error(f'{error_preamble()}  Retrieved Schedule not the same as '
                                                            f'Saved Schedule', schedule.__str__(), sched2.__str__())
    assert len(person_fix.entry_schedules) == len(schedule_fix), attr_error(
        f'{error_preamble()} Schedule Dict Len is wrong ', len(schedule_fix), len(person_fix.entry_schedules))


@pytest.mark.Person
@pytest.mark.Schedule
def test_person_rmv_schedule(person_fix, schedule_fix):
    """
    Tests biometrics_tracker.model.datapoints.Person.remove_entry_sched method

    :param person_fix: a fixture providing a biometrics_tracking.model.datapoint.Person object
    :type person_fix: biometrics_tracking.model.datapoints.Person
    :param schedule_fix: a fixture providing a list of biometrics_tracker.model.datapoints.ScheduleEntry objects
    :type schedule_fix: list[biometrics_tracker.model.datapoints.ScheduleEntry]
    :return: None

    """
    def error_preamble() -> str:
        return f'ScheduleEntry.next_occurrence_today Frequency {schedule.frequency.name} ' \
               f'Starts On {schedule.starts_on.strftime(DATE_FMT)} Ends On {schedule.ends_on.strftime(DATE_FMT)} ' \
               f'Interval {schedule.interval} When : {schedule.when_time}'

    for idx, datum in enumerate(schedule_fix):
        seq_nbr: int = idx + 1
        schedule = dp.ScheduleEntry(datum.person_id, seq_nbr, datum.frequency, datum.dp_type, datum.note,
                                    datum.weekdays, datum.days_of_month, datum.interval, datum.when_time,
                                    datum.starts_on, datum.ends_on, datum.suspended, datum.last_triggered)
        person_fix.add_entry_sched(schedule)

    sched_len = len(person_fix.entry_schedules)

    for seq_nbr in range(1, sched_len+1):
        schedule = person_fix.entry_schedules[seq_nbr]
        person_fix.remove_entry_sched(seq_nbr=seq_nbr)
        assert len(person_fix.entry_schedules) == sched_len - 1, attr_error(f'{error_preamble()} Schedule Dict Size',
                                                                            sched_len,
                                                                            len(person_fix.entry_schedules))
        sched_len -= 1
        assert schedule.seq_nbr not in person_fix.entry_schedules, \
            f'{error_preamble()} Schedule not removed from Person Schedule Dict'
    assert len(person_fix.entry_schedules) == 0, attr_error('Schedule Dict Len is wrong ',
                                                            0, len(person_fix.entry_schedules))


@pytest.mark.DataPoint
def test_blood_pressure_init(blood_pressure_data_fix):
    """
    Tests the biometrics_tracking.model.datapoints.BloodPressure init

    :param blood_pressure_data_fix: a fixture providing a the information to create a list of BloodPressure objects
    :type blood_pressure_data_fix: list[collections.namedtuple('bg_data_t', ['systolic', 'diastolic', 'uom'])]
    :return: None

    """
    for datum in blood_pressure_data_fix:
        blood_pressure: dp.BloodPressure = dp.BloodPressure(systolic=datum.systolic, diastolic=datum.diastolic,
                                                            uom=datum.uom)
        assert blood_pressure.systolic == datum.systolic, f'Blood Pressure: Systolic Expected: {datum.systolic} '\
                                                          f'Observed: {blood_pressure.systolic}'
        assert blood_pressure.diastolic == datum.diastolic, f'Blood Pressure: Diastolic Expected: {datum.diastolic} '\
                                                            f'Observed: {blood_pressure.diastolic}'
        assert blood_pressure.uom == datum.uom, f'Blood Pressure: UOM Expected: {datum.uom.name} '\
                                                f'Observed: {blood_pressure.uom.name}'


@pytest.mark.DataPoint
def test_blood_glucose_init(blood_glucose_data_fix):
    """
    Tests the biometrics_tracker.model.datapoints.BloodGlucose init

    :param blood_glucose_data_fix: a fixture providing data to create a list of BloodGlucose objects
    :type blood_glucose_data_fix: list[collections.namedtuple('bg_data_t', ['value', 'uom'])]
    :return: None

    """
    for datum in blood_glucose_data_fix:
        blood_glucose: dp.BloodGlucose = dp.BloodGlucose(value=datum.value, uom=datum.uom)
        assert blood_glucose.value == datum.value, f'Blood Glucose: Value Expected: {datum.value} '\
                                                   f'Observed: {blood_glucose.value}'
        assert blood_glucose.uom == datum.uom, f'Blood Glucose: UOM Expected: {datum.uom.name} '\
                                               f'Observed: {blood_glucose.uom.name}'


@pytest.mark.DataPoint
def test_pulse_init(pulse_data_fix):
    """
    Tests the biometrics_tracker.model.datapoints.Pulse init

    :param pulse_data_fix: a fixture providing data to create a list of Pulse objects
    :type pulse_data_fix: list[collections.namedtuple('bg_data_t', ['value', 'uom'])]
    :return: None

    """
    for datum in pulse_data_fix:
        pulse: dp.Pulse = dp.Pulse(value=datum.value, uom=datum.uom)
        assert pulse.value == datum.value, f'Pulse: Value Expected: {datum.value} Observed: {pulse.value}'
        assert pulse.uom == datum.uom, f'Pulse: UOM Expected: {datum.uom.name} Observed: {pulse.uom.name}'


def dp_error_preamble(person_id: str, taken: datetime, dp_type: dp.DataPointType,) -> str:
    """
    Returns a formatted message fragment to be used in assertion messages for DataPoint tests

    :param person_id: ID of the person related to the DataPoint
    :type person_id: str
    :param taken: the Taken datetime
    :type taken: datetime.datetime
    :param dp_type: DataPointType enum value
    :type dp_type: biometrics_tracker.model.datapoints.DataPointType
    :return: None

    """
    return f'Person ID: {person_id} Taken: {datetime.strftime(DATETIME_FMT)} Type: {dp_type.name} '


def common_dp_assertions(datum, datapoint: dp.DataPoint):
    """
    Assertions that are common the testing of all DataPoint subclasses.

    :param datum: the data necessary to create a particular class of DataPoint
    :type datum: collections.namedtuple('bg_dp_data_t', ['person_id', 'taken', 'note', 'data', 'type'])
    :param datapoint: a DataPoint instance
    :type datapoint: biometrics_tracking.model.datapoints.DataPoint subclass
    :return: None

    """
    assert datapoint.person_id == datum.person_id, \
        f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
        f'Person ID mismatch: Expected: {datum.preson_id} Observed: {datapoint.person_id}'
    assert compare_mdyhms(datapoint.taken, datum.taken), \
        f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
        f'Person ID mismatch: Expected: {datum.preson_id} Observed: {datapoint.person_id}'
    assert datapoint.note == datum.note, f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
        f'Note mismatch: Expected: {datum.note} Observed: {datapoint.note}'
    assert datapoint.type == datum.type, f'{dp_error_preamble(datum.person_id, datum.taken, datum.type)} ' \
        f'DataPointType mismatch: Expected: {datum.type.name} Observed: {datapoint.type.name}'
    assert datapoint.data.__class__ == datum.data.__class__, \
        f'{dp_error_preamble(datum.person_id, datum.taken, datum.type)} ' \
        f'Metric Class mismatch Expected: {datum.data.__class__.__name__} Observed: {datapoint.__class__.__name__}'
    assert datapoint.data.uom == datum.data.uom, f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
        f'UOM mismatch: Expected: {datum.data.uom.name} Observed: {datapoint.data.uom.name}'


@pytest.mark.DataPoint
@pytest.mark.BloodGlucose
def test_blood_glucose_dp(blood_glucose_dp_data_fix):
    """
    Tests the biometrics_tracker.model.datapoints.BloodGluecoseDP init

    :param blood_glucose_dp_data_fix: a fixture the provides a list containing info to create BloodGlucoseDP objects
    :type blood_glucose_dp_data_fix: collections.namedtuple('bg_dp_data_t', ['person_id', 'taken', 'note', 'data', 'type'])
    :return: None

    """
    for datum in blood_glucose_dp_data_fix:
        datapoint: dp.BloodGlucoseDP = dp.BloodGlucoseDP(datum.person_id, datum.taken, datum.note, datum.data,
                                                         datum.type)
        common_dp_assertions(datum, datapoint)
        dp_data = datapoint.data
        assert dp_data.value == datum.data.value, \
            f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
            f'Value mismatch: Expected: {datum.value} Observed: {dp_data.value}'


@pytest.mark.DataPoint
@pytest.mark.BloodPressure
def test_blood_pressure_dp(blood_pressure_dp_data_fix):
    """
    Tests the biometrics_tracker.model.datapoints.BloodPressureDP init

    :param blood_pressure_dp_data_fix: a fixture the provides a list containing info to create BloodPressureDP objects
    :type blood_pressure_dp_data_fix: collections.namedtuple('bg_dp_data_t', ['person_id', 'taken', 'note', 'data', 'type'])
    :return: None

    """
    for datum in blood_pressure_dp_data_fix:
        datapoint: dp.BloodPressureDP = dp.BloodPressureDP(datum.person_id, datum.taken, datum.note, datum.data,
                                                           datum.type)
        common_dp_assertions(datum, datapoint)
        dp_data = datapoint.data
        assert dp_data.systolic == datum.data.systolic, \
            f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
            f'Systolic mismatch: Expected: {datum.data.systolic} Observed: {dp_data.systolic}'
        assert dp_data.diastolic == datum.data.diastolic, \
            f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
            f'Diastolic mismatch: Expected: {datum.data.diastolic} Observed: {dp_data.diastolic}'


@pytest.mark.DataPoint
@pytest.mark.Pulse
def test_pulse_dp(pulse_dp_data_fix):
    """
    Tests the biometrics_tracker.model.datapoints.PulseDP init

    :param pulse_dp_data_fix: a fixture the provides a list containing info to create PulseDP objects
    :type pulse_dp_data_fix: collections.namedtuple('bg_dp_data_t', ['person_id', 'taken', 'note', 'data', 'type'])
    :return: None

    """
    for datum in pulse_dp_data_fix:
        datapoint: dp.PulseDP = dp.PulseDP(datum.person_id, datum.taken, datum.note, datum.data, datum.type)
        common_dp_assertions(datum, datapoint)
        dp_data = datapoint.data
        assert dp_data.value == datum.data.value, \
            f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
            f'Value mismatch: Expected: {datum.value} Observed: {dp_data.value}'


@pytest.mark.DataPoint
@pytest.mark.BodyTemperature
def test_body_temp_dp(body_temp_dp_data_fix):
    """
    Tests the biometrics_tracker.model.datapoints.BodyTemperatureDP init

    :param body_temp_dp_data_fix: a fixture the provides a list containing info to create BodyTemperatureDP objects
    :type body_temp_dp_data_fix: collections.namedtuple('bg_dp_data_t', ['person_id', 'taken', 'note', 'data', 'type'])
    :return: None

    """
    for datum in body_temp_dp_data_fix:
        datapoint: dp.BodyTemperatureDP = dp.BodyTemperatureDP(datum.person_id, datum.taken, datum.note, datum.data,
                                                               datum.type)
        common_dp_assertions(datum, datapoint)
        dp_data = datapoint.data
        assert dp_data.value == datum.data.value, \
            f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
            f'Value mismatch: Expected: {datum.value} Observed: {dp_data.value}'


@pytest.mark.DataPoint
@pytest.mark.BodyWeight
def test_body_weight_dp(body_weight_dp_data_fix):
    """
    Tests the biometrics_tracker.model.datapoints.BodyWeightDP init

    :param body_weight_dp_data_fix: a fixture the provides a list containing info to create BodyWeightDP objects
    :type body_weight_dp_data_fix: collections.namedtuple('bg_dp_data_t', ['person_id', 'taken', 'note', 'data', 'type'])
    :return: None

    """
    for datum in body_weight_dp_data_fix:
        datapoint: dp.BodyWeightDP = dp.BodyWeightDP(datum.person_id, datum.taken, datum.note, datum.data, datum.type)
        common_dp_assertions(datum, datapoint)
        dp_data = datapoint.data
        assert dp_data.value == datum.data.value, \
            f'{dp_error_preamble(datum.person_id, datum.taken, datum.dp_type)} ' \
            f'Value mismatch: Expected: {datum.value} Observed: {dp_data.value}'

