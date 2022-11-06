import re
import ttkbootstrap as ttkb
from ttkbootstrap.validation import validator

alpha_re = re.compile('\s*[0123456789.]*[a-z, A-Z]+[0123456789.]*\s*')
numeric_re = re.compile('\s*[0123456789.]+\s*')

@validator
def month_validator(event: ttkb.validation.ValidationEvent) -> bool:
    """
    Validates a month value to be numeric and between 1 and 12 inclusive

    :param event:
    :type event: ttkbootstrap.validation.ValidationEvent
    :return: Is the value valid?
    :rtype: bool

    """
    month_str: str = event.postchangetext
    if month_str.isnumeric():
        month: int = int(month_str)
        if 1 <= month <= 12:
            return True
        else:
            return False
    else:
        return False


@validator
def day_validator(event: ttkb.validation.ValidationEvent) -> bool:
    """
    Validates a day value to be numeric and between 1 and 31, inclusive.  A more complete validation is done in the
    :method gui.widgets.DateWidget.validate_date(event): method.

    :param event:
    :type event:  ttkbootstrap.validation.ValidationEvent
    :return: Is the value valid?
    :rtype: bool

    """
    day_str: str = event.postchangetext
    if day_str.isnumeric():
        day: int = int(day_str)
        if 1 <= day <= 31:
            return True
        else:
            return False

    else:
        return False


@validator
def year_validator(event: ttkb.validation.ValidationEvent) -> bool:
    """
    Validates a year value to be numeric

    :param event:
    :type event:  ttkbootstrap.validation.ValidationEvent
    :return: Is the value valid?
    :rtype: bool

    """
    day_str: str = event.postchangetext
    if day_str.isnumeric():
        return True
    else:
        return False


@validator
def hour_validator(event: ttkb.validation.ValidationEvent) -> bool:
    """
    Validate an hour value to be numeric and between 1 and 12, inclusive

    :param event:
    :type event:  ttkbootstrap.validation.ValidationEvent
    :return: Is the value valid?
    :rtype: bool

    """
    hour_str: str = event.postchangetext
    if hour_str.isnumeric():
        hour = int(hour_str)
        if 1 <= hour <= 12:
            return True
        else:
            return False
    else:
        return False


@validator
def minute_validator(event: ttkb.validation.ValidationEvent) -> bool:
    """
    Validate a minute value to be numeric and less than or equal to 59

    :param event:
    :type event:  ttkbootstrap.validation.ValidationEvent
    :return: Is the value valid?
    :rtype: bool

    """
    minute_str: str = event.postchangetext
    if minute_str.isnumeric():
        minute = int(minute_str)
        if minute <= 59:
            return True
        else:
            return False
    else:
        return False

@validator
def numeric_validator(event: ttkb.validation.ValidationEvent) -> bool:
    """
    Validates a value to be numeric

    :param event:
    :type event:  ttkbootstrap.validation.ValidationEvent
    :return: Is the value valid?
    :rtype: bool

    """
    value_str: str = event.postchangetext
    if len(value_str) > 0 and alpha_re.match(value_str) is None and numeric_re.match(value_str) is not None:
        return True
    else:
        return False


@validator
def not_blank_validator(event: ttkb.validation.ValidationEvent) -> bool:
    """
    Validates a value to be non-blank

    :param event:
    :type event:  ttkbootstrap.validation.ValidationEvent
    :return: Is the value valid?
    :rtype: bool

    """
    value_str: str = event.postchangetext
    if len(value_str.strip()) > 0:
        return True
    else:
        return False
