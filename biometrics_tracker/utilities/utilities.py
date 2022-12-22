from collections import UserString
from datetime import date, time, datetime
import importlib.util as imputil
import pathlib
import re
from typing import Any, Optional


time_re = re.compile('(\d{1,2})[:](\d{2})[:](\d{0,2})\s*')
camel_case_re = re.compile('([A-Z]+[a-z]+)')


def mk_datetime(dt: date, tm: time) -> datetime:
    """
    Create a datetime using separate date and time values

    :param dt: date value
    :type dt: datetime.date
    :param tm: time value
    :type tm: datetime.time
    :return: datetime value
    :rtype: datetime.datetime

    """
    return datetime(dt.year, dt.month, dt.day, tm.hour, tm.minute, tm.second)


def split_datetime(dttm: datetime) -> (date, time):
    """
    Return date and time instances created from the proved datetime

    :param dttm: datetime to be split into date and time values
    :type dttm: datetime.datetime
    :return: a tuple containing a date and a time
    :rtype: tuple[datetime.date, datetime.time]

    """
    dt: date = date(dttm.year, dttm.month, dttm.day)
    tm: time = time(dttm.hour, dttm.minute, dttm.second)
    return dt, tm


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


def ordinal(number: int):
    """
    Converts an integer to its ordinal representation

    :param number: integer to be converted
    :type number: int
    :return: ordinal form of the specified number
    :rtype: str

    """

    if number not in [11, 12, 13]:
        mask = (number // 10 * 10)
        ones = number - mask
        if 0 <= ones < 4:
            match ones:
                case 0:
                    out = f'{number:2d}th'
                case 1:
                    out = f'{number:2d}st'
                case 2:
                    out = f'{number:2d}nd'
                case 3:
                    out = f'{number:2d}rd'
        else:
            out = f'{number:<2d}th'
    else:
        out = f'{number:<2d}th'
    return out


def datetime_from_str(datetime_str: str) -> datetime:
    """
    Returns a datetime created from the ISO format string passed to the function

    :param datetime_str: an ISO formatted date/time string
    :type datetime_str: str
    :return: a date/time
    :rtype: datetime.datetime
    """
    dattim: Optional[datetime] = None
    if datetime_str is not None and len(datetime_str.strip()) > 0:
        dattim = datetime.fromisoformat(datetime_str)
    return dattim


def time_from_str(time_str: str) -> time:
    """
    Returns a time created from a string formated as HH:MM:SS.  This function uses the following regular expression
    to validate the string format and extract hours, minutes and seconds from the string:
    '(\d{1,2})[:](\d{2})[:](\d{0,2})\s*'

    :param time_str: the time, in string form
    :type time_str: str
    :return: a time
    :rtype: datetime.datetime

    """
    tim: Optional[time] = None
    re_result = time_re.match(time_str)
    if re_result is not None and len(re_result.groups()) == 3:
        h, m, s = re_result.groups()
        hour = int(h)
        minute = int(m)
        second = int(s)
        tim = time(hour, minute, second)
    else:
        raise ValueError
    return tim


def increment_date(dt: date, years: int = 0, months: int = 0, days: int = 0) -> date:
    """
    Increments the provided date by the specified number of years, months and days.  The increment values may be
    positive or negative, but they must all have the same sign or a ValueError exception will be raised.
    :param dt: the date to be incremented
    :type dt: datetime.date
    :param years: the number of years to increment
    :type years: int
    :param months: the number of months to increment
    :type months: int
    :param days: the number of days to increment
    :type days: int
    :return: the resulting date
    :rtype: datetime.date
    """
    if days < 0 and (months > 0 or years > 0):
        raise ValueError('Increment years, months and days must all have the same sign')
    elif days > 0 and (months < 0 or years < 0):
        raise ValueError('Increment years, months and days must all have the same sign')
    year = dt.year
    month = dt.month
    day = dt.day
    if days >= 0:
        day += days
        while day > max_dom_map[month]:
            day -= max_dom_map[month]
            month += 1
        month += months
        while month > 12:
            month -= 12
            year += 1
        year += years
    else:
        day += days
        while day <= 0:
            month -= 1
            if month == 0:
                month = 12
            day += max_dom_map[month]
        month -= months
        while month <= 0:
            month += 12
            year -= 1
    return date(year=year, month=month, day=day)


def increment_datetime(dattim: datetime, years: int = 0, months: int = 0, days: int = 0) -> datetime:
    """
    Increments the provided date by the specified number of years, months and days.  The increment values may be
    positive or negative, but they must all have the same sign or a ValueError exception will be raised.
    :param dattim: the datetime to be incremented
    :type dattim: datetime.datetime
    :param years: the number of years to increment
    :type years: int
    :param months: the number of months to increment
    :type months: int
    :param days: the number of days to increment
    :type days: int
    :return: the resulting date
    :rtype: datetime.date
    """
    dt: date = increment_date(dattim.date(), years, months, days)
    return mk_datetime(dt, dattim.time())


max_dom_map: dict[int, int] = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}


def max_dom(month: int) -> int:
    """
    Returns the number of days in the specified month
    :param month: the month for which the number of days will be returned
    :type month: int
    :return: the number of days in the month
    :rtype: int
    """
    if month in range(1, 13):
        return max_dom_map[month]
    else:
        return 0


def step_enumerate(lst: list[Any], start: int = 0, step: int = 1) -> list[tuple[int, Any]]:
    """
    This function is similar to the built in enumerate function, but it allows you to specify  a starting value and
    the amount to increment the index for each item in the list

    :param lst: a list of items to be enumerated
    :type lst: list[Any]
    :param start: the starting value for the enumeration
    :type start: int
    :param step: the amount by which the index will be incremented for each itme in the list
    :type step: int
    :return: a list of tuples consisting of index/item pairs
    :rtype: list[tuple[int, Any]]

    """
    enum_list: list[tuple[int, Any]] = []
    idx: int = start
    for entry in lst:
        enum_list.append((idx, entry))
        idx += step
    return enum_list


def split_camelcase(camelcase: str) -> list[str]:
    """
    Splits a camelcase string into its components
    :param camelcase: a camelcase formatted string
    :type camelcase: str
    :return: a list consisting of the components of the original string
    :rtype: list[str]

    """
    groups: list[str] = camel_case_re.split(camelcase)
    return [w for w in groups if len(w) > 0]


def whereami(package_name: str) -> pathlib.Path:
    """
    Returns a Path object pointing to the location of the application's package folder
    
    :return: the path of the application's package directory
    :rtype: pathlib.Path

    """
    origin = imputil.find_spec(package_name).origin
    return pathlib.Path(origin).parent


class MutableString(UserString):
    def __setitem__(self, index: int, value: str) -> None:
        data_as_list = list(self.data)
        data_as_list[index] = value
        self.data = "".join(data_as_list)

    def __delitem__(self, index: int) -> None:
        data_as_list = list(self.data)
        del data_as_list[index]
        self.data = "".join(data_as_list)

    def append(self, string: str) -> None:
        data_as_list = list(self.data)
        data_as_list.extend(list(string))
        self.data = "".join(data_as_list)

