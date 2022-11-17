from datetime import datetime, date, time
from decimal import Decimal
import logging
import re
from typing import Any, Optional

TIME_FMT: str = '%I:%M:%S %p'
DATE_FMT: str = '%m/%d/%Y'
DATETIME_FMT: str = f'{DATE_FMT} {TIME_FMT}'

mdy_re = re.compile('"*(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})"*')
ymd_re = re.compile('"*(\d{2,4})[/-](\d{1,2})[/-](\d{1,2})"*')
time12_re = re.compile('"*(\d{1,2}):(\d{2}):(\d{0,2})\s*([am|AM|pm|PM]{2})"*')
time24_re = re.compile('"*(\d{1,2}):(\d{2}):(\d{0,2})\s*"*')
int_value_re = re.compile('"*(\d{2,4})([\s\w]*)"*')
dec_value_re = re.compile('"*(\d{2,4}[.]*\d*)([\s\w]*)"*')


def attr_error(attr: str, expected: Any, observed: Any):
    """
    Returns a boiler-plate message fragment consisting of an attribute name and expected and observed values

    :param attr: attribute name
    :type attr: str
    :param expected: the expected value
    :type expected: Any
    :param observed: the observed value
    :type observed: Any
    :return: None

    """
    return f'{attr}: Expected {expected} Observed {observed}'


def compare_hms(time1, time2) -> bool:
    """
    Compares the hour, minutes and seconds of two datetime.time objects.  This avoids false inequalities based on
    microsecond level variances

    :param time1: the first of a pair of times to be compared
    :type time1: datetime.time
    :param time2: the second of a pair of times to be compared
    :type time2: datetime.time
    :return: are the times equal
    :rtype:  bool

    """
    if time1.hour == time2.hour and time1.minute == time2.minute and time1.second == time2.second:
        return True
    else:
        return False


def compare_mdyhms(datetime1: datetime, datetime2: datetime) -> bool:
    """
    Compares the day, month, year, hour, minutes and seconds of two datetime.datetime objects.  This avoids false
    inequalities based on microsecond level variances

    :param datetime1: the first of a pair of datetimes to be compared
    :type datetime1: datetime.datetime
    :param datetime2: the second of a pair of datetimes to be compared
    :type datetime2: datetime.datetime
    :return: are the datetimes equal
    :rtype: bool

    """
    if datetime1.month == datetime2.month and datetime1.day == datetime2.day and datetime1.year == datetime2.year:
        return compare_hms(datetime1.time(), datetime2.time())
    else:
        return False


def compare_object(obj1: Any, obj2: Any) -> bool:
    """
    Compare two objects by comparing the entries in their __dict__ property

    :param obj1: the first of a pair of objects to be compared
    :type obj1: Any
    :param obj2: the second of a pair of objects to be compared
    :type obj2: Any
    :return: are the objects equal
    :rtype: bool

    """
    if isinstance(obj2, obj1.__class__) and obj1.__dict__ == obj2.__dict__:
        return True
    else:
        for key, value in obj1.__dict__.items():
            if key not in obj2.__dict__:
                return False
            elif not isinstance(value, (Decimal, float)):
                if value != obj2.__dict__[key]:
                    return False
                else:
                    return True
            elif abs(value - obj2.__dict__[key]) > 0.1:
                return False
            else:
                return True
        return False


def dict_diff(name: str, exp_list: dict, obs_list: dict):
    """
    Formats a message fragment to be used if the contents of two dicts are unequal

    :param name: the name of the attribute that the dicts represent
    :type name: str
    :param exp_list: a dict containing the expected values
    :type exp_list: dict
    :param obs_list: a dict containing the observed values
    :return: a message fragment
    :rtype: str

    """
    expected = ','.join(f'{str(k)}{str(v)}' for k, v in exp_list.items())
    observed = ','.join(f'{str(k)}{str(v)}' for k, v in obs_list.items())
    return f'{name} list mismatch Expected: {expected}, Observed: {observed}'


def decode_date(logger: logging.Logger, date_fmt: str, date_str: str) -> Optional[date]:
    """
    Decode a date value from the provided string, using the format specified the provided format string.  This
    method raises a ValueError exception if a date can't be derived from the provided string value

    :param logger: a logger that will be written to if the decode fails
    :type logger: logging.Logger
    :param date_fmt: the strftime format spec with which the date string was formatted
    :type date_fmt: str
    :param date_str: the formatted date string
    :type date_str: str
    :return: the date value decoded from the date string
    :rtype: Optional[datetime.date]

    """
    try:
        if date_fmt[0:2] in ['%m', '%d']:
            match = mdy_re.match(date_str)
            if match is not None:
                groups = match.groups()
                if date_fmt[0:2] == '%m':
                    return date(year=int(groups[2]), month=int(groups[0]), day=int(groups[1]))
                else:
                    return date(year=int(groups[2]), month=int(groups[1]), day=int(groups[0]))
        else:
            match = ymd_re.match(date_str)
            if match is not None:
                groups = match.groups()
                return date(year=int(groups[0]), month=int(groups[1]), day=int(groups[2]))
        raise ValueError(f'Date: {date_str} could not be decoded using format {date_fmt}')
    except ValueError:
        error_msg: str = f'Date: {date_str} could not be decoded using format {date_fmt}'
        logger.error(error_msg)
        raise ValueError(error_msg)


def decode_time(logger: logging.Logger, time_fmt: str, time_str: str) -> Optional[time]:
    """
    Decode a time value from the provided string, using the format specified the provided format string. A
    ValueError exception is raised if a time can't be derived from the provided string

    :param logger: a logger that will be written to if the decode fails
    :type logger: logging.Logger
    :param time_fmt: the strftime format spec with which the time string was formatted
    :type time_fmt: str
    :param time_str: the formatted time string
    :type time_str: str
    :return: the time value decoded from the time string
    :rtype: Optional[datetime.time]

    """
    try:
        if time_fmt[0:2] == '%H':
            match = time24_re.match(time_str)
            if match is not None:
                groups = match.groups()
                return time(hour=int(groups[0]), minute=int(groups[1]), second=int(groups[2]))
        else:
            match = time12_re.match(time_str)
            if match is not None:
                groups = match.groups()
                hour = int(groups[0])
                if len(groups) == 4:
                    if groups[3] in ['pm', 'PM'] and hour < 12:
                        hour += 12
                    elif groups[3] in ['am', 'AM'] and hour == 12:
                        hour = 0
                return time(hour=hour, minute=int(groups[1]), second=int(groups[2]))
        raise ValueError(f'Time: {time_str} could not be decoded using format {time_fmt}')
    except ValueError:
        error_msg: str = f'Time: {time_str} could not be decoded using format {time_fmt}'
        logger.error(error_msg)
        raise ValueError(error_msg)


def decode_decimal(dec_str: str) -> tuple[Decimal, str]:
    """
    Decodes a Decimal value from the provided string value  This method will properly handle strings in which a
    U.O.M. is appended to the Decimal value

    :param dec_str: a string representation of a Decimal value
    :type dec_str: str
    :return: a Decimal value
    :rtype: decimal.Decimal

    """
    match = dec_value_re.match(dec_str.strip())
    uom_str: str = ''
    if match is None:
        return Decimal(0), uom_str
    groups = match.groups()
    if len(groups) > 1:
        uom_str = groups[1]
    return Decimal(float(groups[0])), uom_str


def decode_int(int_str: str) -> tuple[int, str]:
    """
    Decodes an integer from the provided string value.  This method will properly handle strings in which a
    U.O.M. is appended to the integer value

    :param int_str: a string representation of an integer
    :type int_str: str
    :return: an integer value
    :rtype: int

    """
    match = int_value_re.match(int_str.strip())
    if match is not None:
        groups = match.groups()
        uom_str = ''
        if groups is None:
            return 0, uom_str
        elif len(groups) > 1:
            uom_str = groups[1]
        return int(groups[0]), uom_str
