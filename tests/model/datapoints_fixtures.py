import collections
from datetime import date, time, datetime, timedelta
from decimal import Decimal
import pytest
from typing import Any, Union

import tests.model.random_data as rd

import biometrics_tracker.model.datapoints as dp

"""
    This module contains three types of functions:

+   *functions named xxx_data()* return named tuples of properties required for the creation of a particular
    biometrics_tracker.model.datapoints class instance.  These functions are used by other functions in this
    module.  The data for these classes is randomized by calling methods of the test.RandomData or
    test.model.BiometricsRandomData classes

+   *functions named xxx_data_fix()* are wrappers around classes of the type described above.  This wrapper pattern is
    used because functions annotated as pytest fixtures don't work properly when invoked directly

+   *functions named xxx()* (e.g. person()) return instances of particular biometrics_tracker.model.datapoints classes,
    using the xxx_data() functions as a data source.  As with the xxx_data functions, these are used by functions
    annotated as fixtures

+   *functions named xxx_fix()* return biometrics_tracking.model.datapoints class instances and are annotated as pytest
    fixtures.

"""

random_data = rd.BiometricsRandomData()

DP_MAX = 10


def person_data():
    """

    :return: a named tuple containing the properties needed to create a biometrics_tracker.model.datapoints.Person
    :rtype: collections.namedtuple('person_data_t', ['id', 'name', 'dob', 'age'])
    """
    data = collections.namedtuple('person_data_t', ['id', 'name', 'dob', 'age'])
    data.id = random_data.random_string(5, initial_caps=False, no_spaces=True)
    data.name = f'{random_data.random_alpha_string(6)} {random_data.random_alpha_string(10)}'
    data.dob = random_data.random_dob()
    age_dur = date.today() - data.dob
    data.age = age_dur.days // 365
    return data


@pytest.fixture
def person_data_fix():
    """
    Wraps person_data.  This function is annotated as a pytest.fixture

    :return: a named tuple containing the properties needed to create a biometrics_tracker.model.datapoints.Person
    :rtype: collections.namedtuple('person_data_t', ['id', 'name', 'dob', 'age'])
    """
    return person_data()


def person(person_data_fix) -> dp.Person:
    """

    :param person_data_fix: a pytest.fixture providing the data to create a biometrics_tracker.model.datapoints.Person instance
    :type person_data_fix: pytest.fixture
    :return: a Person instance created from randomized data
    :rtype: biometrics_tracker.model.datapoints.Person

    """
    person = dp.Person(id=person_data_fix.id, name=person_data_fix.name, dob=person_data_fix.dob,
                       age=person_data_fix.age)
    person.tracked.clear()
    return person


@pytest.fixture
def person_fix(person_data_fix) -> dp.Person:
    """
    A pytest.fixture wrapper for person()

    :param person_data_fix: a pytest.fixture providing the data to create a biometrics_tracker.model.datapoints.Person instance
    :return: a Person instance created from randomized data
    :rtype: biometrics_tracker.model.datapoints.Person

    """
    return person(person_data_fix)


def people():
    """
    Creates a list of biometrics_tracker.model.datapoints.Person instances using randomized data. The created instances
    are placed in a dict keyed by ID to prevent the creation of Person instances with duplicated IDs

    :return:  dict.values() containing the biometrics_tracker.model.datapoints.Person instances
    """
    people_map: dict[str, dp.Person] = {}
    for idx in range(1, random_data.random_int(5, 20)):
        joe = person(person_data())
        if joe.id not in people_map:
            people_map[joe.id] = joe
    return people_map.values()


@pytest.fixture
def people_fix():
    """
    A pytest.fixture wrapper for people()

    :return: dict.values() containing the biometrics_tracker.model.datapoints.Person instances

    """
    return people()


def tracking_config_data():
    """
    Creates a list of named tuples containing the data necessary to create a set of TrackingConfig instances

    :return: a list of named tuples containing the data necessary to create a set of TrackingConfig instances
    :rtype: list[collections.namedtuple('tracking_config_data_t', ['dp_type', 'default_uom', 'tracked'])]

    """
    data: list = []
    dp_types_used: list[dp.DataPointType] = []
    idx: int = 0
    while idx < 5:
        datum = collections.namedtuple('tracking_config_data_t', ['dp_type', 'default_uom', 'tracked'])
        datum.dp_type = random_data.random_dp_type()
        if datum.dp_type not in dp_types_used:
            datum.default_uom = random_data.random_uom(datum.dp_type)
            datum.tracked = random_data.random_bool()
            data.append(datum)
            dp_types_used.append(datum.dp_type)
            idx += 1
    return data


@pytest.fixture
def tracking_config_fix():
    """
    A pytest.fixture that uses tracking_config_data to create a list of TrackingConfig instances

    :return: a list of TrackingConfig instances
    :rtype: list[biometrics_tracker.model.datapoints.TrackingConfig]

    """
    data: list[dp.TrackingConfig] = []
    for datum in tracking_config_data():
        data.append(dp.TrackingConfig(dp_type=datum.dp_type, default_uom=datum.default_uom, tracked=datum.tracked))
    return data


def schedule_data():
    """
    Creates a list of named tuples containing the data necessary to create a set of ScheduleEntry instances

    :return: a list of named tuples containing the data necessary to create a set of ScheduleEntry instances
    :rtype: list[collections.namedtuple('schedule_data_t', ['person_id', 'seq_nbr', 'frequency', 'dp_type', 'weekdays',
                                                           'days_of_month', 'interval', 'when_time', 'starts_on',
                                                           'ends_on',
                                                           'suspended', 'last_triggered'])]
    """
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
    """
    A pytest.fixture that uses tracking_config_data to create a list of Schedule instances

    :return: a list of ScheduleEntry instances
    :rtype: list[biometrics_tracker.model.datapoints.ScheduleEntry]

    """
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
    """
    Creates named tuple of data necessary to create an instance of one of the metrics classes (e.g. BloodPressure,
    BloodGlucose, etc.)  using random values within the specified ranges

    :param value_lower: a lower limit for the random metric value
    :type value_lower: Union[int, decimal.Decimal]
    :param value_upper: a upper limit for the random metric value
    :type value_upper: Union[int, decimal.Decimal]
    :param dp_type: the DataPointType for the metric
    :param precision: for Decimal values, the number of places to the right of the decimal point
    :return: a named tuple of properties to create an instance of one of the metrics classes
    :rtype:  collections.namedtuple('bg_data_t', ['value', 'uom'])

    """
    datum = collections.namedtuple('bg_data_t', ['value', 'uom'])
    if isinstance(value_lower, int):
        datum.value = random_data.random_int(value_lower, value_upper)
    else:
        datum.value = Decimal(random_data.random_dec(value_lower, value_upper, precision))
    datum.uom = random_data.random_uom(dp_type)
    return datum


def make_datapoint(person_id: str, taken_lower: datetime, taken_upper: datetime, note: str, dp_type: dp.DataPointType,
                   data: dp.metric_union):
    """
    Creates named tuple of data necessary to create an instance of one of the DataPoint classes (e.g. BloodPressureDP,
    BloodGlucoseDP, etc.)  using random values within the specified ranges

    :param person_id: the Person ID to be associated with the DataPoint data
    :type person_id: str
    :param taken_lower: a lower limit for the Taken datetime property
    :type taken_lower: datetime
    :param taken_upper: an upper limit for the Taken datetime property
    :type taken_upper: datetime
    :param note: a note to be associated with the DataPoint data
    :type note: str
    :param dp_type: the DataPointType to be associated with the DataPoint data
    :type dp_type: biometrics_tracker.model.datapoints.DataPointType
    :param data: an instance the metric class associated with the DataPointType
    :type: metric_union
    :return: a named tuple containing the data to create an instance of one of the DataPoint classes
    :rtype: collections.namedtuple('bg_dp_data_t', ['person_id', 'taken', 'note', 'data', 'type'])

    """
    datum = collections.namedtuple('bg_dp_data_t', ['person_id', 'taken', 'note', 'data', 'type'])
    datum.person_id = person_id
    datum.taken = random_data.random_datetime(taken_lower, taken_upper)
    datum.note = note
    datum.type = dp_type
    datum.data = data
    return datum


def blood_glucose_data():
    """
    Uses the make_metric function to assemble a list data for the biometrics_tracker.model.datapoints.BloodGlucose
    class

    :return: list[collections.namedtuple]

    """
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, DP_MAX)):
        data.append(make_metric(40, 2000, dp.DataPointType.BG))
    return data


@pytest.fixture
def blood_glucose_data_fix():
    """
    A pytest.fixture wrapper for blood_glucose_data()

    :return: list[collections.namedtuple]

    """
    return blood_glucose_data()


@pytest.fixture
def blood_glucose_dp_data_fix():
    """
    Uses the make_datapoint function to assemble a list data for the biometrics_tracker.model.datapoints.BloodGlucoseDP
    class

    :return: list[collections.namedtuple]

    """
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now()
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for datum in blood_glucose_data():
        bg: dp.BloodGlucose = dp.BloodGlucose(datum.value, datum.uom)
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.BG, bg))
    return data


def blood_pressure_data():
    """
    Uses the make_metric function to assemble a list data for the biometrics_tracker.model.datapoints.BloodPressure
    class

    :return: list[collections.namedtuple]

    """
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, DP_MAX)):
        datum = collections.namedtuple('bg_data_t', ['systolic', 'diastolic', 'uom'])
        datum.systolic = random_data.random_int(70, 400)
        datum.diastolic = random_data.random_int(40, 200)
        datum.uom = random_data.random_uom(dp.DataPointType.BP)
    return data


@pytest.fixture
def blood_pressure_data_fix():
    """
    A pytest.fixture wrapper for blood_pressure_data()

    :return: list[collections.namedtuple]

    """
    return blood_pressure_data()


@pytest.fixture
def blood_pressure_dp_data_fix():
    """
    Uses the make_datapoint function to assemble a list data for the biometrics_tracker.model.datapoints.BloodPressureDP
    class

    :return: list[collections.namedtuple]

    """
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now()
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for datum in blood_pressure_data():
        bp: dp.BloodPressure = dp.BloodPressure(datum.systolic, datum.diastolic, datum.uom)
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.BP, bp))
    return data


def pulse_data():
    """
    Uses the make_metric function to assemble a list data for the biometrics_tracker.model.datapoints.Pulse
    class

    :return: list[collections.namedtuple]

    """
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, DP_MAX)):
        data.append(make_metric(50, 200, dp.DataPointType.PULSE))
    return data


@pytest.fixture
def pulse_data_fix():
    """
    A pytest.fixture wrapper for pulse_data()

    :return: list[collections.namedtuple]

    """
    return pulse_data()


@pytest.fixture
def pulse_dp_data_fix() -> list[collections.namedtuple]:
    """
    Uses the make_datapoint function to assemble a list data for the biometrics_tracker.model.datapoints.PulseDP
    class

    :return: list[collections.namedtuple]

    """
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now()
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for datum in pulse_data():
        pulse: dp.Pulse = dp.Pulse(datum.value, datum.uom)
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.PULSE, pulse))
    return data


def body_temp_data() -> list[collections.namedtuple]:
    """
    Uses the make_metric function to assemble a list data for the biometrics_tracker.model.datapoints.BodyTemp
    class

    :return: list[collections.namedtuple]

    """
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, 5)):
        data.append(make_metric(Decimal(90.0), Decimal(110.0), dp.DataPointType.BODY_TEMP))
    return data


@pytest.fixture
def body_temp_data_fix() -> list[collections.namedtuple]:
    """
    A pytest.fixture wrapper for body_temp_data()

    :return: list[collections.namedtuple]

    """
    return body_temp_data()


@pytest.fixture
def body_temp_dp_data_fix() -> list[collections.namedtuple]:
    """
    Uses the make_datapoint function to assemble a list data for the
    biometrics_tracker.model.datapoints.BodyTemperatureDP class

    :return: list[collections.namedtuple]

    """
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now()
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for datum in body_temp_data():
        body_temp: dp.BodyTemperature = dp.BodyTemperature(datum.value, datum.uom)
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.BODY_TEMP, body_temp))
    return data


def body_weight_data() -> list[collections.namedtuple]:
    """
    Uses the make_metric function to assemble a list data for the biometrics_tracker.model.datapoints.BodyWeight
    class

    :return: list[collections.namedtuple]

    """
    data: list[collections.namedtuple] = []
    for idx in range(random_data.random_int(1, DP_MAX)):
        data.append(make_metric(Decimal(10.0), Decimal(1100.0), dp.DataPointType.BODY_WGT))
    return data


@pytest.fixture
def body_weight_data_fix() -> list[collections.namedtuple]:
    """
    A pytest.fixture wrapper for body_weight_data()

    :return: list[collections.namedtuple]

    """
    return body_weight_data()


@pytest.fixture
def body_weight_dp_data_fix() -> list[collections.namedtuple]:
    """
    Uses the make_datapoint function to assemble a list data for the biometrics_tracker.model.datapoints.BodyWeightDP
    class

    :return: list[collections.namedtuple]

    """
    person_id = random_data.random_string(length=5)
    taken_lower: datetime = datetime.now() - timedelta(days=45)
    taken_upper: datetime = datetime.now()
    note = random_data.random_string(length=25)
    data: list[collections.namedtuple] = []
    for datum in body_weight_data():
        body_weight: dp.BodyWeight = dp.BodyWeight(datum.value, datum.uom)
        data.append(make_datapoint(person_id, taken_lower, taken_upper, note, dp.DataPointType.BODY_WGT, body_weight))
    return data


@pytest.fixture
def datapoints_fix(people_fix, blood_pressure_dp_data_fix, pulse_dp_data_fix, blood_glucose_dp_data_fix,
                   body_temp_dp_data_fix, body_weight_dp_data_fix) -> list[dp.DataPoint]:
    """
    A pytest.fixture that provides a varied list of DataPoint subclass instances using rondom data

    :param people_fix: a pytest.fixture providing Person instances
    :param blood_pressure_dp_data_fix: a pytest.fixture providing data to create BloodPressureDP instances
    :param pulse_dp_data_fix: a pytest.fixture providing data to create PulseDP instances
    :param blood_glucose_dp_data_fix: a pytest.fixture providing data to crate BloodGlucoseDP instances
    :param body_temp_dp_data_fix: a pytest.fixture providing data to create BodyTemperature instances
    :param body_weight_dp_data_fix: a pytest.fixture providing data to create BodyWeight instances
    :return: a list of DataPoint subclasses
    :rtype: list[biometrics_tracking.model.datapoints.DataPoint]

    """
    def grab_dp_data() -> list[collections.namedtuple]:
        switch: int = random_data.random_int(1, 5)
        match switch:
            case 1:
                return blood_pressure_dp_data_fix
            case 2:
                return pulse_dp_data_fix
            case 3:
                return blood_glucose_dp_data_fix
            case 4:
                return body_temp_dp_data_fix
            case 5:
                return body_weight_dp_data_fix
            case _:
                raise ValueError(f'switch value {switch} out of range')

    def make_key(taken: datetime, type: dp.DataPointType) -> str:
        return f'{taken.strftime("%m/%d/%Y %HH:%MM:%SS")}:{type.name}'

    datapoints: list[dp.DataPoint] = []
    for person in people_fix:
        datapoints_map: dict[str, str] = {}
        for _ in range(0, 2):
            for datum in grab_dp_data():
                match datum.type:
                    case dp.DataPointType.BG:
                        data: dp.BloodGlucose = dp.BloodGlucose(value=datum.data.value, uom=datum.data.uom)
                    case dp.DataPointType.BP:
                        data: dp.BloodPressure = dp.BloodPressure(systolic=datum.data.systolic,
                                                                  diastolic=datum.data.diastolic, uom=datum.data.uom)
                    case dp.DataPointType.PULSE:
                        data: dp.Pulse = dp.Pulse(value=datum.data.value, uom=datum.data.uom)
                    case dp.DataPointType.BODY_TEMP:
                        data: dp.BodyTemperature(value=datum.data.value, uom=datum.data.uom)
                    case dp.DataPointType.BODY_WGT:
                        data: dp.BodyWeight = dp.BodyWeight(value=datum.data.value, uom=datum.data.uom)
                    case _:
                        raise ValueError(f'DP Type {datum.type} out of range')
                key = make_key(datum.taken, datum.type)
                if key not in datapoints_map:
                    datapoints.append(dp.DataPoint(person_id=person.id, taken=datum.taken, note=datum.note, data=datum.data,
                                                   type=datum.type))
                    datapoints_map[key] = 'x'

    return datapoints





