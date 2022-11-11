import collections
from datetime import date, time, datetime, timedelta
from decimal import Decimal
import pytest
from typing import Any, Union

import tests.model.random_data as rd

import biometrics_tracker.model.datapoints as dp

random_data = rd.BiometricsRandomData()


def person_data():
    data = collections.namedtuple('person_data_t', ['id', 'name', 'dob', 'age'])
    data.id = random_data.random_string(5, False)
    data.name = f'{random_data.random_alpha_string(6)} {random_data.random_alpha_string(10)}'
    data.dob = random_data.random_dob()
    age_dur = date.today() - data.dob
    data.age = age_dur.days // 365
    return data


@pytest.fixture
def person_data_fix():
    return person_data()


def person(person_data_fix) -> dp.Person:
    person = dp.Person(id=person_data_fix.id, name=person_data_fix.name, dob=person_data_fix.dob,
                       age=person_data_fix.age)
    person.tracked.clear()
    return person


@pytest.fixture
def person_fix(person_data_fix) -> dp.Person:
    return person(person_data_fix)


def people():
    people_map: dict[str, dp.Person] = {}
    for idx in range(1, random_data.random_int(5, 20)):
        joe = person(person_data())
        if joe.id not in people_map:
            people_map[joe.id] = joe
    return people_map.values()


@pytest.fixture
def people_fix():
    return people()


def tracking_config_data():
    data: list = []
    dp_types_used: list[dp.DataPointType] = []
    idx: int = 0
    while idx < 5:
        datum = collections.namedtuple('tracking_config_data_t', ['dp_type', 'uom', 'tracked'])
        datum.dp_type = random_data.random_dp_type()
        if datum.dp_type not in dp_types_used:
            datum.uom = random_data.random_uom(datum.dp_type)
            datum.tracked = random_data.random_bool()
            data.append(datum)
            dp_types_used.append(datum.dp_type)
            idx += 1
    return data


@pytest.fixture
def tracking_config_fix():
    data: list[dp.TrackingConfig] = []
    for datum in tracking_config_data():
        data.append(dp.TrackingConfig(dp_type=datum.dp_type, uom=datum.uom, tracked=datum.tracked))
    return data


def schedule_data():
    data: list = []
    person_id: str = random_data.random_string(5)
    seq_nbr: int = 1
    for idx in range(random_data.random_int(1, 5)):
        datum = collections.namedtuple('schedule_data_t', ['person_id', 'seq_nbr', 'frequency', 'dp_type', 'weekdays',
                                                           'days_of_month', 'interval', 'when_time', 'starts_on',
                                                           'ends_on',
                                                           'suspended', 'last_triggered'])
        datum.person_id = person_id
        datum.seq_nbr = seq_nbr
        seq_nbr += 1
        datum.frequency = random_data.random_frequency_type()
        datum.dp_type = random_data.random_dp_type()
        datum.note = random_data.random_alpha_string(20)
        datum.weekdays = []
        datum.days_of_month = []
        lower = datetime.today() - timedelta(days=random_data.random_int(15, 30))
        upper = datetime.today() + timedelta(days=random_data.random_int(15, 30))
        datum.starts_on, datum.ends_on = random_data.random_date_pair(lower, upper)
        if datum.frequency == dp.FrequencyType.Weekly:
            datum.weekdays = random_data.random_weekdays(max_days=5)
        elif datum.frequency == dp.FrequencyType.Monthly:
            datum.days_of_month = random_data.random_dom(month=datum.starts_on.month, max_dom=12)
        datum.interval = random_data.random_int(1, 4)
        datum.when_time = random_data.random_time(time(hour=6, minute=0, second=0), time(hour=20, minute=0, second=0))
        datum.suspended = random_data.random_bool()
        datum.last_triggered = random_data.random_datetime(datetime(year=lower.year, month=lower.month, day=lower.day,
                                                           hour=0, minute=0, second=0),
                                                           datetime.now())

        data.append(datum)
    return data


@pytest.fixture
def schedule_fix():
    data: list[dp.ScheduleEntry] = []
    for datum in schedule_data():
        data.append(dp.ScheduleEntry(person_id=datum.person_id, seq_nbr=datum.seq_nbr, frequency=datum.frequency,
                                     starts_on=datum.starts_on, ends_on=datum.ends_on, dp_type=datum.dp_type,
                                     note=datum.note, weekdays=datum.weekdays, days_of_month=datum.days_of_month,
                                     interval=datum.interval, when_time=datum.when_time, suspended=datum.suspended,
                                     last_triggered=datum.last_triggered))
    return data


def make_metric(value_lower: Union[int, Decimal], value_upper: Union[int, Decimal], dp_type: dp.DataPointType,
                precision: int = 2) -> collections.namedtuple:
    datum = collections.namedtuple('bg_data_t', ['value', 'uom'])
    if isinstance(value_lower, int):
        datum.value = random_data.random_int(value_lower, value_upper)
    else:
        datum.value = random_data.random_dec(value_lower, value_upper, precision)
    datum.uom = random_data.random_uom(dp_type)
    return datum


def make_datapoint(person_id: str, taken_lower: datetime, taken_upper: datetime, note: str, dp_type: dp.DataPointType,
                   data: Any):
    datum = collections.namedtuple('bg_dp_data_t', ['person_id', 'taken', 'note', 'data', 'type'])
    datum.person_id = person_id
    datum.taken = random_data.random_datetime(taken_lower, taken_upper)
    datum.note = note
    datum.type = dp_type
    datum.data = data
    return datum


def blood_glucose_data():
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, 5)):
        data.append(make_metric(40, 2000, dp.DataPointType.BG))
    return data


@pytest.fixture
def blood_glucose_data_fix():
    return blood_glucose_data()


@pytest.fixture
def blood_glucose_dp_data_fix():
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now() - timedelta(days=45)
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for bg in blood_glucose_data():
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.BG, bg))
    return data


def blood_pressure_data():
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, 5)):
        datum = collections.namedtuple('bg_data_t', ['systolic', 'diastolic', 'uom'])
        datum.systolic = random_data.random_int(70, 400)
        datum.diastolic = random_data.random_int(40, 200)
        datum.uom = random_data.random_uom(dp.DataPointType.BP)
    return data


@pytest.fixture
def blood_pressure_data_fix():
    return blood_pressure_data()


@pytest.fixture
def blood_pressure_dp_data_fix():
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now() - timedelta(days=45)
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for bp in  blood_pressure_data():
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.BP, bp))
    return data


def pulse_data():
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, 5)):
        data.append(make_metric(50, 200, dp.DataPointType.BG))
    return data


@pytest.fixture
def pulse_data_fix():
    return pulse_data()


@pytest.fixture
def pulse_dp_data_fix() -> list[collections.namedtuple]:
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now() - timedelta(days=45)
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for pulse in pulse_data():
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.PULSE, pulse))
    return data


def body_temp_data() -> list[collections.namedtuple]:
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, 5)):
        data.append(make_metric(Decimal(90.0), Decimal(110.0), dp.DataPointType.BODY_TEMP))
    return data


@pytest.fixture
def body_temp_data_fix() -> list[collections.namedtuple]:
    return body_temp_data()


@pytest.fixture
def body_temp_dp_data_fix() -> list[collections.namedtuple]:
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now() - timedelta(days=45)
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for body_temp in body_temp_data():
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.PULSE, body_temp))
    return data


def body_weight_data() -> list[collections.namedtuple]:
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, 5)):
        data.append(make_metric(Decimal(10.0), Decimal(1100.0), dp.DataPointType.BODY_WGT))
    return data


@pytest.fixture
def body_weight_data_fix() -> list[collections.namedtuple]:
    return body_weight_data()


@pytest.fixture
def body_weight_dp_data_fix() -> list[collections.namedtuple]:
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now() - timedelta(days=45)
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for body_weight in body_weight_data():
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.PULSE, body_weight))
    return data


