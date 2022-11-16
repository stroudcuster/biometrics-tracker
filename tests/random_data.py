
from datetime import date, time, datetime, timedelta
from decimal import Decimal, getcontext
import random
from time import sleep
from typing import Any, Optional, Tuple

TIME_FMT: str = '%I:%M:%S %p'
DATE_FMT: str = '%m/%d/%Y'
DATETIME_FMT: str = f'{DATE_FMT} {TIME_FMT}'


class RandomData:
    """
    This class provides a variety of randomized data, maintaining the seed as a property so that, if necessary it
    can be queried and used to provide the same sequence of data in a later test run.
    """
    max_dom_map: dict[int, int] = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30,
                                   12: 31}

    def __init__(self, seed: Optional[int] = None):
        """
        Creates and instance of RandomData

        :param seed: an optional parameter the allows a seed value used in the past to be used again
        :type seed: int

        """
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        else:
            random.seed()
        self.init_state = random.getstate()

    def get_init_state(self):
        """
        Retrieves the randon.getstate() value that was saved when the RandomData instance was created

        :return: the initial state of the random number generator
        :rtype: tuple[Any]

        """
        return self.init_state

    def get_current_state(self) -> tuple[Any, ...]:
        """
        Retrieves the current state of the random number generator

        :return: the current state of the random number generator
        :rtype: tuple[Any, ...]

        """
        return random.getstate()

    def set_state(self, state: tuple[Any, ...], set_as_initial: bool):
        """
        Set the random number generator state to a particular value

        :param state: state to be established
        :type state: tuple[Any, ...]
        :param set_as_initial: should this state be saved in the initial state property
        :type set_as_initial: bool
        :return: None

        """
        random.setstate(state)
        if set_as_initial:
            self.init_state = state

    @staticmethod
    def random_int(lower: int, upper: int):
        """
        Returns a random integer within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: int
        :param upper: upper boundary
        :type upper: int
        :return: random int value
        :rtype: int

        """
        if upper > lower:
            return random.randint(lower, upper)
        elif upper == lower:
            return upper
        else:
            raise ValueError(f'Upper bound ({upper:d}) greater than lower bound ({lower:d}).')

    @staticmethod
    def random_int_pair(lower: int, upper: int) -> tuple[int, int]:
        """
        Returns a sorted tuple of random int values within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: int
        :param upper: upper boundary
        :type upper: int
        :return: a pair of sorted int values
        :rtype: tuple[int]

        """
        if upper > lower:
            ints: list[int] = [random.randint(lower, upper), random.randint(lower, upper)]
            ints.sort()
            return ints[0], ints[1]
        elif upper == lower:
            return lower, upper
        else:
            raise ValueError(f'Upper bound ({upper:d}) greater than lower bound ({lower:d}).')

    @staticmethod
    def random_dec(lower: [float, Decimal], upper: [float, Decimal], precision: int = 2):
        """
        Returns a random Decimal within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: float or decimal.Decimal
        :param upper: upper boundary
        :type upper: float or decimal.Decimal
        :return: random Decimal value
        :rtype: decimal.Decimal

        """
        if upper > lower:
            if isinstance(lower, Decimal):
                float_lower: float = lower.__float__()
            else:
                float_lower = lower
            if isinstance(upper, Decimal):
                float_upper: float = upper.__float__()
            else:
                float_upper = upper
            return round(Decimal.from_float(random.uniform(float_lower, float_upper)), precision)
        else:
            raise ValueError(f'Upper bound ({upper:.2f}) greater than lower bound ({lower:.2f}).')

    @staticmethod
    def random_dec_pair(self, lower: [float, Decimal], upper: [float, Decimal], precision: int = 2):
        """
        Returns a sorted tuple of random Decimal values within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: float or decimal.Decimal
        :param upper: upper boundary
        :type upper: float or decimal.Decimal
        :return: a pair of sorted int values
        :rtype: tuple[decimal.Decimal]

        """
        if upper > lower:
            if isinstance(lower, Decimal):
                float_lower: float = lower.__float__()
            else:
                float_lower = lower
            if isinstance(upper, Decimal):
                float_upper: float = upper.__float__()
            else:
                float_upper = upper
            floats = [random.uniform(float_lower, float_upper), random.uniform(float_lower, float_upper)]
            floats.sort()
            return round(Decimal(floats[0]), precision), round(Decimal(floats[1]), precision)
        else:
            raise ValueError(f'Upper bound ({upper}) must be greater than lower bound ({lower}).')

    @staticmethod
    def random_date(lower: date, upper: date) -> date:
        """
        Returns a random datetime.date within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: datetime.date
        :param upper: upper boundary
        :type upper: datetime.date
        :return: random date value
        :rtype: datetime.date

        """
        if upper > lower:
            year: int = random.randint(lower.year, upper.year)
            month: int = 1
            day: int = 1
            if lower.year == upper.year:
                month = random.randint(lower.month, upper.month)
            else:
                month = random.randint(1, 12)
            if month in RandomData.max_dom_map:
                try:
                    day = random.randint(1, RandomData.max_dom_map[month])
                except KeyError:
                    day = 1
                # Some invalid days of month are sneaking by
                if day > RandomData.max_dom_map[month]:
                    day = RandomData.max_dom_map[month]
            rtn_value: Optional[date] = None
            try:
                rtn_value = date(year, month, day)
            except ValueError:
                raise ValueError(f'Day {day} not valid for month {month}')
            return rtn_value
        else:
            raise ValueError(f'Upper bound ({upper.strftime(DATE_FMT)}) '
                             f'must be after lower bound({lower.strftime(DATE_FMT)})')

    @staticmethod
    def random_date_pair(lower: date, upper: date):
        """
        Returns a sorted pair random datetime.date within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: datetime.date
        :param upper: upper boundary
        :type upper: datetime.date
        :return: random date value
        :rtype: tuple[datetime.date, datetime.date]

        """
        if upper > lower:
            dates: list[date] = [RandomData.random_date(lower, upper), RandomData.random_date(lower, upper)]
            dates.sort()
            return dates[0], dates[1]
        else:
            raise ValueError(f'Upper bound ({upper.strftime(DATE_FMT)}) '
                             f'must be after lower bound({lower.strftime(DATE_FMT)})')

    @staticmethod
    def random_time(lower: time, upper: time):
        """
        Returns a random datetime.time within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: datetime.time
        :param upper: upper boundary
        :type upper: datetime.time
        :return: random time value
        :rtype: datetime.time

        """
        if upper > lower:
            second: int = 0
            hour: int = random.randint(lower.hour, upper.hour)
            if lower.hour == upper.hour:
                minute: int = random.randint(lower.minute, upper.minute)
                if lower.minute == upper.minute:
                    second = random.randint(lower.second, upper.second)
            else:
                second = -1
                if hour == lower.hour:
                    minute = random.randint(lower.minute, 59)
                    if minute == lower.minute:
                        second = random.randint(lower.second, 59)
                elif hour == upper.hour:
                    minute = random.randint(0, upper.minute)
                    if minute == upper.minute:
                        second = random.randint(0, upper.second)
                else:
                    minute = random.randint(0, 59)
                if second < 0:
                    second = random.randint(0, 59)
            return time(hour, minute, second)
        else:
            ValueError(f'Upper bound ({upper.strftime(TIME_FMT)}) '
                       f'must be after lower bound({lower.strftime(TIME_FMT)})')

    @staticmethod
    def random_time_pair(lower: time, upper: time) -> tuple[time, time]:
        """
        Returns a sorted pair random datetime.time within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: datetime.time
        :param upper: upper boundary
        :type upper: datetime.time
        :return: random time value
        :rtype: tuple[datetime.time, datetime.time]

        """
        if upper > lower:
            times: list[time] = [RandomData.random_time(lower, upper), RandomData.random_time(upper, lower)]
            times.sort()
            return times[0], time[1]
        else:
            ValueError(f'Upper bound ({upper.strftime(TIME_FMT)}) '
                       f'must be after lower bound({lower.strftime(TIME_FMT)})')

    @staticmethod
    def random_datetime(lower: datetime, upper: datetime) -> datetime:
        """
        Returns a random datetime.datetime within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: datetime.datetime
        :param upper: upper boundary
        :type upper: datetime.datetime
        :return: random datetime value
        :rtype: datetime.datetime

        """
        rnd_time: Optional[time] = None
        if upper > lower:
            if upper.date() > lower.date():
                rnd_date: date = RandomData.random_date(lower.date(), upper.date())
            elif lower.date() == upper.date():
                rnd_date = lower.date()
                rnd_time: time = RandomData.random_time(lower.time(), upper.time())
            if rnd_time is None:
                rnd_time: time = RandomData.random_time(time(hour=0, minute=0, second=0), time(hour=23, minute=59,
                                                                                               second=59))
            return datetime(year=rnd_date.year, month=rnd_date.month, day=rnd_date.day,
                            hour=rnd_time.hour, minute=rnd_time.minute, second=rnd_time.second)
        else:
            ValueError(f'Upper bound ({upper.strftime(DATETIME_FMT)}) '
                       f'must be after lower bound({lower.strftime(DATETIME_FMT)}')

    @staticmethod
    def random_datetime_pair(lower: datetime, upper: datetime) -> tuple[datetime, datetime]:
        """
        Returns a sorted pair random datetime.datetime within  a range expressed by inclusive upper and lower bounds

        :param lower: lower boundary
        :type lower: datetime.datetime
        :param upper: upper boundary
        :type upper: datetime.datetime
        :return: random time value
        :rtype: tuple[datetime.datetime, datetime.datetime]

        """
        if upper > lower:
            datetimes: list[datetime] = [RandomData.random_datetime(lower, upper), RandomData.random_datetime(lower,
                                                                                                              upper)]
            datetimes.sort()
            return datetimes[0], datetimes[1]
        else:
            ValueError(f'Upper bound ({upper.strftime(DATETIME_FMT)}) '
                       f'must be after lower bound({lower.strftime(DATETIME_FMT)})')

    @staticmethod
    def random_string(length: int, initial_caps: bool = True, no_spaces: bool = False) -> str:
        string = ''
        for idx in range(length):
            char: str = random.choice("abcdefghijklmnopqrstuvwxyz 0123456789")
            if not no_spaces or char != ' ':
                string = f'{string}{char}'
        if initial_caps:
            return f'{string[0:1].upper()}{string[1:]}'
        else:
            return string

    @staticmethod
    def random_alpha_string(length: int, initial_caps: bool = True, no_spaces: bool = False) -> str:
        """
        Returns a string of random alpha characters

        :param length: the length of the string to be returned
        :type length: int
        :param initial_caps: should the initial alpha char in the string be capitalized
        :type initial_caps: bool
        :param no_spaces: are spaces allowed in the returned string
        :type no_spaces: bool
        :return: the produced string
        :rtype: str

        """
        string = ''
        for idx in range(length):
            char: str = random.choice("abcdefghijklmnopqrstuvwxyz ")
            if not no_spaces or char != ' ':
                string = f'{string}{char}'
        if initial_caps:
            return f'{string[0:1].upper()}{string[1:]}'

    @staticmethod
    def random_bool() -> bool:
        """
        Returns a random boolean value

        :return: produced bool value
        :rtype: bool

        """
        bool_value: bool = True
        if random.random() > .5:
            bool_value = False
        return bool_value

    @staticmethod
    def random_dict_item(dictx: dict) ->tuple[Any, Any]:
        """
        Returns a randomly chosen key/value pair from the provided dict

        :param dictx: the dict from which a key/value pair will be selected
        :type dictx: dict
        :return: a key/value pair
        :rtype: tuple[Any, Any]

        """
        keys = [key for key in dictx.keys()]
        idx = RandomData.random_int(0, len(keys)-1)
        return keys[idx], dictx[keys[idx]]
