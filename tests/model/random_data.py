from datetime import date
import tests.random_data as rd
from typing import Optional

import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.uoms as uoms


class BiometricsRandomData(rd.RandomData):
    """
    A subclass of tests.RandomData the provides random values particular to the biometrics_tracking package

    """
    def __init__(self, seed: Optional[int] = None):
        """
        Creates a testing.model.RandomData instance

        :param seed: the seed value to be passed onto the parent class
        :type seed: int

        """
        rd.RandomData.__init__(self,  seed)
        self.dp_type_len: int = len(dp.DataPointType)
        self.frequency_type_len: int = len(dp.FrequencyType)
        self.weekday_len: int = len(dp.WeekDay)

    def random_dob(self) -> date:
        """
        Provides a random date in the range 1/1/1940 to 12/31/1970

        :return:  a random date
        :rtype: datetime.date

        """
        return self.random_date(date(1940, 1, 1), date(1970, 12, 31))

    def random_uom(self, dp_type: dp.DataPointType) -> uoms.UOM:
        """
        Returns a random UOM enum value appropriate to the provided DataPointType

        :param dp_type: the DataPointType to be used in determining which UOM enum class is to be used
        :type dp_type: biometrics_tracking.model.datapoints.DataPointType
        :return: a random UOM enum value
        :rtype: a subclass of biometrics_tracking.model.datapoints.UOM value

        """
        uom = dp.dptype_uom_map[dp_type]
        idx = self.__class__.random_int(1, len(uom))
        return uom(idx)

    def random_dp_type(self) -> dp.DataPointType:
        """
        Returns a random DataPointType enum value

        :return: random DataPointType
        :rtype: biometrics_tracker.model.datapoints.DataPointType

        """
        return dp.DataPointType(self.__class__.random_int(1, self.dp_type_len))

    def random_frequency_type(self) -> dp.FrequencyType:
        """
        Returns a random FrequencyType for a ScheduleEntry

        :return: random FrequencyType
        :rtype: biometrics_tracker.model.datapoints.FrequencyType

        """
        return dp.FrequencyType(self.__class__.random_int(1, self.frequency_type_len))

    def random_weekdays(self, max_days) -> list[dp.WeekDay]:
        """
        A list of randomly chosen Weekday enum values.

        :param max_days:  the maximum number of Weekday enum values to be returned
        :type max_days: int
        :return: a list of Weekday enum values
        :rtype: list[biometrics_tracker.model.datapoints.Weekday]

        """
        if 1 <= max_days <= 7:
            weekdays: list[dp.WeekDay] = []
            while len(weekdays) <= max_days:
                day: dp.WeekDay = dp.WeekDay(self.random_int(1, 7))
                if day not in weekdays:
                    weekdays.append(day)
            return weekdays
        else:
            raise ValueError(f'Max Days must be 1 - 7 : value {max_days}')

    def random_dom(self, month: int, max_dom: int) -> list[int]:
        """
        Returns a randomly chosen list of days of the month

        :param month: the month to be used as an upper boundary for values to be returned
        :type month: int
        :param max_dom: the maximum number of days to be returned
        :type max_dom: int
        :return: a list of days of the month
        :rtype: list[int]

        """
        if 1 <= month <= 12:
            doms: list[int] = []
            while len(doms) <= max_dom:
                dom = self.random_int(1, BiometricsRandomData.max_dom_map[month])
                if dom not in doms:
                    doms.append(dom)
            return doms
        else:
            raise ValueError(f'Month must be 1 - 12 value {month:d}')


