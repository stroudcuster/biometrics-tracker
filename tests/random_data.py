
from datetime import date, time, datetime, timedelta
from decimal import Decimal, getcontext
import random
from time import sleep
from typing import Optional

TIME_FMT: str = '%I:%M:%S %p'
DATE_FMT: str = '%m/%d/%Y'
DATETIME_FMT: str = f'{DATE_FMT} {TIME_FMT}'


class RandomData:
    max_dom_map: dict[int, int] = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30,
                                   12: 31}

    def __init__(self, seed: Optional[int] = None):
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        else:
            random.seed()
        self.init_state = random.getstate()

    def get_init_state(self):
        return self.init_state

    def get_current_state(self):
        return random.getstate()

    def set_state(self, state, set_as_initial: bool):
        random.setstate(state)
        if set_as_initial:
            self.init_state = state

    @staticmethod
    def random_int(lower: int, upper: int):
        if upper > lower:
            return random.randint(lower, upper)
        elif upper == lower:
            return upper
        else:
            raise ValueError(f'Upper bound ({upper:d}) greater than lower bound ({lower:d}).')

    @staticmethod
    def random_int_pair(lower: int, upper: int) -> tuple[int, int]:
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
            return date(year, month, day)
        else:
            raise ValueError(f'Upper bound ({upper.strftime(DATE_FMT)}) '
                             f'must be after lower bound({lower.strftime(DATE_FMT)})')

    @staticmethod
    def random_date_pair(lower: date, upper: date):
        if upper > lower:
            dates: list[date] = [RandomData.random_date(lower, upper), RandomData.random_date(lower, upper)]
            dates.sort()
            return dates[0], dates[1]
        else:
            raise ValueError(f'Upper bound ({upper.strftime(DATE_FMT)}) '
                             f'must be after lower bound({lower.strftime(DATE_FMT)})')

    @staticmethod
    def random_time(lower: time, upper: time):
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
        if upper > lower:
            times: list[time] = [RandomData.random_time(lower, upper), RandomData.random_time(upper, lower)]
            times.sort()
            return times[0], time[1]
        else:
            ValueError(f'Upper bound ({upper.strftime(TIME_FMT)}) '
                       f'must be after lower bound({lower.strftime(TIME_FMT)})')

    @staticmethod
    def random_datetime(lower: datetime, upper: datetime) -> datetime:
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
        if upper > lower:
            datetimes: list[datetime] = [RandomData.random_datetime(lower, upper), RandomData.random_datetime(lower,
                                                                                                              upper)]
            datetimes.sort()
            return datetimes[0], datetimes[1]
        else:
            ValueError(f'Upper bound ({upper.strftime(DATETIME_FMT)}) '
                       f'must be after lower bound({lower.strftime(DATETIME_FMT)})')

    @staticmethod
    def random_string(length: int, initial_caps: bool = True) -> str:
        string = ''
        for idx in range(length):
            string = f'{string}{random.choice("abcdefghijklmnopqrstuvwxyz 0123456789")}'
        if initial_caps:
            return f'{string[0:1].upper()}{string[1:]}'
        else:
            return string

    @staticmethod
    def random_alpha_string(length: int, initial_caps: bool = True) -> str:
        string = ''
        for idx in range(length):
            string = f'{string}{random.choice("abcdefghijklmnopqrstuvwxyz ")}'
        if initial_caps:
            return f'{string[0:1].upper()}{string[1:]}'

    @staticmethod
    def random_bool() -> bool:
        bool_value: bool = True
        if random.random() > .5:
            bool_value = False
        return bool_value

    @staticmethod
    def random_dict_item(dictx: dict):
        keys = [key for key in dictx.keys]
        idx = RandomData.random_int(0, len(keys)-1)
        return keys[idx], dictx[keys[idx]]
