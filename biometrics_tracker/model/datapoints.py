from dataclasses import dataclass, field
from decimal import Decimal
from datetime import date, datetime, time
from enum import Enum, auto
from typing import Any, Iterator, Optional, Union

import biometrics_tracker.model.uoms as uoms
import biometrics_tracker.utilities.utilities as utilities


@dataclass
class BodyWeight:
    """
    Contains properties to record a body weight, exclusive of date and time
    :type uom: Union(type(biometrics_tracker.model.uoms.Weight.POUNDS),
    type(biometrics_tracker.model.uoms.Weight.KILOS)

    """
    value: Decimal
    uom: Union[type(uoms.Weight.POUNDS), type(uoms.Weight.KILOS)]

    @staticmethod
    def label() -> str:
        """
        Provides a string label to be used for screens and output

        :return: a label for use on displays and in reports
        :rtype: str

        """
        return "Body Weight"

    def __str__(self) -> str:
        """
        Returns a string representation containing the recorded value and the u.o.m. abbreviation

        :return: a value and unit of measure abbreviation
        :rtype: str

        """
        return f"{self.value:.1f} {self.uom.abbreviation()}"


@dataclass
class BloodGlucose:
    """
    Contains properties to record a blood glucose level, exclusive of date and time
    """
    value: int
    uom: Union[type(uoms.BG.MG_PER_DL), type(uoms.BG.MMOL_PER_L)]

    @staticmethod
    def label() -> str:
        """
        Provides a string label to be used for screens and output

        :return: a label for use on displays and in reports
        :rtype: str

        """
        return "Blood Glucose"

    def __str__(self) -> str:
        """
        Returns a string representation containing the recorded value and the u.o.m. abbreviation

        :return: a value and unit of measure abbreviation
        :rtype: str

        """
        return f"{self.value} {self.uom.abbreviation()}"


@dataclass
class BloodPressure:
    """
    Contains properties to record a blood pressure, exclusive of date and time
    """
    systolic: int
    diastolic: int
    uom: Union[type(uoms.Pressure.MM_OF_HG)]

    @staticmethod
    def label() -> str:
        """
        Provides a string label to be used for screens and output

        :return: a label for use on displays and in reports
        :rtype: str

        """
        return "Blood Pressure"

    def __str__(self) -> str:
        """
        Returns a string representation containing the systolic and diastolic values and the u.o.m. abbreviation

        :return: a value and unit of measure abbreviation
        :rtype: str

       """
        return f"{self.systolic}/{self.diastolic} {self.uom.abbreviation()}"


@dataclass
class Pulse:
    """
    Contains properties to record a pulse rate, exclusive of date and time

    """
    value: int
    uom: Union[type(uoms.Rate.BPM)]

    @staticmethod
    def label() -> str:
        """
        Provides a string label to be used for screens and output

        :return: a label for use on displays and in reports
        :rtype: str

       """
        return "Pulse"

    def __str__(self):
        """
        Returns a string representation containing the recorded value and the u.o.m. abbreviation

        :return: a value and unit of measure abbreviation
        :rtype: str

        """
        return f"{self.value:3d} {self.uom.abbreviation()}"


@dataclass
class BodyTemperature:
    """
    Contains properties to record a body temperature, exclusive of date and time
    """
    value: Decimal
    uom: Union[type(uoms.Temperature.CELSIUS), type(uoms.Temperature.FAHRENHEIT)]

    @staticmethod
    def label() -> str:
        """
        Provides a string label to be used for screens and output

        :return: a label for use on displays and in reports
        :rtype: str

        """
        return "Temperature"

    def __str__(self):
        """
        Returns a string representation containing the recorded value and the u.o.m. abbreviation

        :return: a value and unit of measure abbreviation
        :rtype: str

        """
        return f"{self.value:.1f} {self.uom.abbreviation()}"


class DataPointType(Enum):
    """
    This Enum enumerates each of the types of datapoints supported by the application

    """
    # Body Weight
    BODY_WGT = auto()
    # Blood Glucose
    BG = auto()
    # Blood Pressure
    BP = auto()
    # Pulse
    PULSE = auto()
    # Body Temperature
    BODY_TEMP = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields Weight.POUNDS, Weight.KILOS, etc
        :rtype: biometrics_tracker.model.datapoints.DataPointType

        """
        for i in range(1, DataPointType.__len__()+1):
            yield DataPointType(i)


class TrackingConfig:
    """
    Indicates that a particular datapoint is recorded for a particular person

    """
    def __init__(self, dp_type: DataPointType, default_uom: uoms.UOM, tracked: bool):
        self.dp_type: DataPointType = dp_type
        self.default_uom: uoms.UOM = default_uom
        self.tracked: bool = tracked


class FrequencyType(Enum):
    """
    An Enum subclass that represents the various frequencies a ScheduleEntry can have; Hourly, Daily, Weekly, etc.
    """
    Hourly = auto()
    Daily = auto()
    Weekly = auto()
    Monthly = auto()
    One_Time = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields ScheduleType.HOURLY, ScheduleType.WEEKDAY, etc
        :rtype: biometrics_tracker.model.datapoints.FrequencyType

        """
        for i in range(1, FrequencyType.__len__() + 1):
            yield FrequencyType(i)


frequency_name_map = {f.name: f for f in FrequencyType}


class WeekDay(Enum):
    """
    An Enum representing the days of the week, for use in scheduling.  Note the int value of each day of the week
    is one greater than the corresponding value returned by the datetime.weekday function.

    """
    Monday = auto()
    Tuesday = auto()
    Wednesday = auto()
    Thursday = auto()
    Friday = auto()
    Saturday = auto()
    Sunday = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields WeekDay.MONDAY, WeekDay.TUESDAY, etc
        :rtype: biometrics_tracker.model.datapoints.WeekDay

        """
        for i in range(1, WeekDay.__len__()+1):
            yield WeekDay(i)


weekday_name_map = {w.name: w for w in WeekDay}


@dataclass
class ScheduleEntry:
    """
    The class contains the information pertaining to a single entry in a periodic schedule.  Frequency types are
    Once, Monthly, Weekly and Hourly.  Schedule entries with type Once specify a date and time.  Schedule entries
    with type Monthly specify one or more day of the month.  Schedule entries with type Weekly specify  one or more
    days of the week.  Monthly, Weekly and Hourly schedule entries also specify an interval parameter, e.g. used to
    schedule events for the 3rd Wed of every month.  All schedules have start and end date/times and record the last
    time the entry was triggered.
    """
    person_id: str
    seq_nbr: int
    frequency: FrequencyType
    dp_type: DataPointType
    note: str
    weekdays: list[WeekDay]
    days_of_month: list[int]
    interval: Optional[int]
    when_time: time
    starts_on: date
    ends_on: date
    suspended: bool = field(default=False)
    last_triggered: Optional[datetime] = field(default=None)

    @classmethod
    def max_day(cls, month: int):
        """
        Get the max number of days in the specified month

        :param month:
        :type month: int
        :return: number of days in the specified month
        :rtype: int

        """
        max_dom: dict[int, int] = {
            1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
        }
        return max_dom[month]

    @staticmethod
    def get_weekday_dates(weekday: WeekDay) -> list[date]:
        """
        Returns a list of dates for a day of the week for the current month

        :param weekday: The weekday for which dates are to be returned
        :type weekday: biometrics_tracking.model.datapoints.EntrySchedule
        :return: the dates within the current month the fall on the specified day of the week
        :rtype: list[date]

        """
        date_list: list[date] = []
        cur_month: int = datetime.now().month
        cur_year: int = datetime.now().year
        for d in range(1, ScheduleEntry.max_day(cur_month)):
            test_date = date(cur_year, cur_month, d)
            if WeekDay(test_date.weekday()+1) == weekday:
                date_list.append(test_date)
        return date_list

    def next_occurrence_today(self, today: date) -> list[datetime]:
        """
        Return a list containing the date/times that the schedule will be triggered on the specified day.

        :param today: the date for which the schedules will be examined
        :type today: datetime
        :return: a list of the trigger date/times
        :rtype: list[datetime]

        """
        next_occur: list[datetime] = []
        if not self.suspended:
            if self.last_triggered is not None:
                mark = self.last_triggered
            if self.starts_on <= today:
                if today <= self.ends_on:
                    match self.frequency:
                        case FrequencyType.One_Time:
                            if self.starts_on == today and self.when_time <= datetime.now().time():
                                next_occur.append(utilities.mk_datetime(self.starts_on, self.when_time))
                        case FrequencyType.Monthly:
                            for day_of_month in self.days_of_month:
                                if day_of_month == today.day and self.when_time <= datetime.now().time():
                                    next_occur.append(utilities.mk_datetime(today, self.when_time))
                        case FrequencyType.Weekly:
                            for weekday in self.weekdays:
                                if weekday == WeekDay(today.weekday()+1):
                                    weekday_dates = self.get_weekday_dates(weekday)
                                    if self.interval == 0:
                                        next_occur.append(utilities.mk_datetime(today, self.when_time))
                                    elif date(today.year, today.month, today.day) in weekday_dates and \
                                            self.when_time <= datetime.now().time():
                                        next_occur.append(utilities.mk_datetime(today, self.when_time))
                        case FrequencyType.Daily:
                            next_occur.append(utilities.mk_datetime(today, self.when_time))
                        case FrequencyType.Hourly:
                            hour: int = datetime.now().time().hour
                            while hour < 24:
                                next_occur.append(datetime(year=today.year, month=today.month, day=today.day, hour=hour,
                                                           minute=self.when_time.minute))
                                hour += self.interval
            return next_occur

    def __str__(self):
        """
        Returns a readable string representation of the schedule's parameters
        """
        stub = f'Seq No: {self.seq_nbr} {self.starts_on.strftime("%m/%d/%y")}-{self.ends_on.strftime("%m/%d/%y")} ' \
               f'{dptype_dp_map[self.dp_type].label()} {self.frequency.name}'
        if self.suspended:
            stub = f'{stub} Suspended'
        match self.frequency:
            case FrequencyType.One_Time:
                when = datetime(self.starts_on.year, self.starts_on.month, self.starts_on.day,
                                self.when_time.hour, self.when_time.minute, self.when_time.second)
                return f'{stub} on {when.strftime("%m/%d/%Y")} at {when.strftime("%I:%M:%S %p")}'
            case FrequencyType.Monthly:
                if len(self.days_of_month) > 0:
                    days: str = ','.join(utilities.ordinal(day) for day in self.days_of_month)
                    return f'{stub} on the {days} days of the month at {self.when_time.strftime("%I:%M:%S %p")}.'
                else:
                    return f'{stub} no days of month specified'
            case FrequencyType.Weekly:
                if len(self.weekdays) > 0:
                    days: str = ','.join(day.name for day in self.weekdays)
                    if self.interval == 1:
                        return f'{stub} every {days} at {self.when_time.strftime("%I:%M:%S %p")}'
                    else:
                        return f'{stub} every {utilities.ordinal(self.interval)} {days} at ' \
                               f'{self.when_time.strftime("%I:%M:%S %p")}'
                else:
                    return f'{stub} no day of week specified at {self.when_time.strftime("%I:%M:%S %p")}'
            case FrequencyType.Daily:
                if self.interval == 1:
                    return f'{stub} at {self.when_time.strftime("%I:%M:%S %p")}'
                else:
                    return f'{stub} at every {utilities.ordinal(self.interval)} day at ' \
                           f'{self.when_time.strftime("%I:%M:%S %p")}'
            case FrequencyType.Hourly:
                if self.interval == 1:
                    return f'{stub} at {self.when_time.strftime("%M:%S")} after the hour'
                else:
                    return f'{stub} every {utilities.ordinal(self.interval)} hour starting at ' \
                           f'{self.when_time.strftime("%M:%S")} after the hour'


@dataclass
class Person:
    """
    Contains the data relevant to a person, including a list of Tracking Config instances to record the
    datapoints tracked for an individual

    """
    id: str
    name: str
    dob: date
    age: int = field(default=0)

    def __post_init__(self):
        self.tracked: dict[str: TrackingConfig] = {}
        self.entry_schedules: dict[int: ScheduleEntry] = {}

    def add_tracked_dp_type(self, dp_type, tc: TrackingConfig):
        self.tracked[dp_type] = tc

    def remove_tracked_dp_type(self, dp_type):
        del self.tracked[dp_type]

    def is_tracked(self, dp_type: DataPointType):
        if dp_type not in self.tracked:
            return False
        else:
            return self.tracked[dp_type].tracked

    def dp_type_track_cfg(self, dp_type: DataPointType) -> TrackingConfig:
        return self.tracked[dp_type]

    def add_entry_sched(self, sched: ScheduleEntry):
        self.entry_schedules[sched.seq_nbr] = sched

    def remove_entry_sched(self, seq_nbr: int):
        del self.entry_schedules[seq_nbr]

    def dp_type_uom(self, dp_type: DataPointType) -> uoms.UOM:
        return self.tracked[dp_type].default_uom

    def __str__(self):
        return f'ID: {self.id} Name: {self.name} D.O.B: {self.dob.strftime("%m/%d/%Y")}'


@dataclass
class DataPoint:
    """
    This a base class for datapoint records.  It includes properties for person ID, the date time the reading
    was taken and a note.

    """
    person_id: str
    taken: datetime
    note: str
    data: Any
    type: DataPointType

    def __str__(self):
        return f'{self.taken.strftime("%m/%d/%Y %I:%M %p")} {dptype_dp_map[self.type].label()} {self.data.__str__()} ' \
               f'{self.note}'


@dataclass
class BodyWeightDP(DataPoint):
    """
    This class implements the complete set of information related to a record of a body weight

    """
    data: BodyWeight
    type: DataPointType = field(default=DataPointType.BODY_WGT)


@dataclass
class BloodPressureDP(DataPoint):
    """
    This class implements the complete set of information related to a record of a blood pressure reading

    """
    data: BloodPressure
    type: DataPointType = field(default=DataPointType.BP)


@dataclass
class PulseDP(DataPoint):
    """
    This class implements the complete set of information related to a record of a pulse reading

    """
    data: Pulse
    type: DataPointType = field(default=DataPointType.PULSE)


@dataclass
class BloodGlucoseDP(DataPoint):
    """
    This class implements the complete set of information related to a record of a blood glucose reading

    """
    data: BloodGlucose
    type: DataPointType = field(default=DataPointType.BG)


@dataclass
class BodyTemperatureDP(DataPoint):
    """
    This class implements the complete set of information related to a record of a body temperature reading

    """
    data: BodyTemperature
    type: DataPointType = field(default=DataPointType.BODY_TEMP)


dptypes_union = BodyWeightDP | BodyTemperatureDP | BloodPressureDP | BloodGlucoseDP | PulseDP

dptype_uom_map = \
    {DataPointType.BG: uoms.BG,
     DataPointType.BODY_TEMP: uoms.Temperature,
     DataPointType.BODY_WGT: uoms.Weight,
     DataPointType.BP: uoms.Pressure,
     DataPointType.PULSE: uoms.Rate}
"""
A dict[model.datapoints.DatapointType, uoms.UOM] that provides a mapping between each model.datapoints.DataPointType 
value and the model.uoms.UOM derived Enum the represents the associated unit of measure

:property dptype_uom_map:

"""

dptype_dp_map = {DataPointType.BG: BloodGlucose,
                 DataPointType.BODY_TEMP: BodyTemperature,
                 DataPointType.BODY_WGT: BodyWeight,
                 DataPointType.BP: BloodPressure,
                 DataPointType.PULSE: Pulse}
"""
A dict[model.datapoints.DataPointType, BloodGlucose, BodyTemperature, etc.] that provides a mapping between each
model.datapoint.DataPointType value and the class that is used to represent the value of the metric.

:property dptype_dp_map:

"""

dptype_name_map = {DataPointType.BG.name: DataPointType.BG,
                   DataPointType.BODY_TEMP.name: DataPointType.BODY_TEMP,
                   DataPointType.BODY_WGT.name: DataPointType.BODY_WGT,
                   DataPointType.BP.name: DataPointType.BP,
                   DataPointType.PULSE.name: DataPointType.PULSE}

"""
A dict[str, model.datapoints.DataPointType] that provides a mapping between the name property of each 
DataPointType value and the corresponding value

:property dptype_name_map:

"""


dptype_label_map = {dptype: dptype_dp_map[dptype].label() for dptype in DataPointType}
"""
A dict[str, model.datapoints.DataPointType] that provides a mapping between the label metric type and the corresponding 
DataPointType 

:property dptype_label_map:

"""


metric_union = Union[BloodGlucose, BloodPressure, Pulse, BodyTemperature, BodyWeight]
"""
A Union type that includes the metric classes (BloodPress, BloodGlucose, etc)

"""



