from datetime import datetime
from decimal import Decimal
from typing import Any

TIME_FMT: str = '%I:%M:%S %p'
DATE_FMT: str = '%m/%d/%Y'
DATETIME_FMT: str = f'{DATE_FMT} {TIME_FMT}'


def attr_error(attr: str, expected: Any, observed: Any):
    return f'{attr}: Expected {expected} Observed {observed}'


def compare_hms(time1, time2) -> bool:
    if time1.hour == time2.hour and time1.minute == time2.minute and time1.second == time2.second:
        return True
    else:
        return False


def compare_mdyhms(datetime1: datetime, datetime2: datetime) -> bool:
    if datetime1.month == datetime2.month and datetime1.day == datetime2.day and datetime1.year == datetime2.year:
        return compare_hms(datetime1.time(), datetime2.time())
    else:
        return False


def compare_object(obj1, obj2) -> bool:
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
