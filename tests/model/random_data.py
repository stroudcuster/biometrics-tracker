from datetime import date
import tests.random_data as rd
from typing import Optional

import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.uoms as uoms


class BiometricsRandomData(rd.RandomData):
    def __init__(self, seed: Optional[int] = None):
        rd.RandomData.__init__(self,  seed)
        self.dp_type_len: int = len(dp.DataPointType)
        self.frequency_type_len: int = len(dp.FrequencyType)
        self.weekday_len: int = len(dp.WeekDay)

    def random_dob(self) -> date:
        return self.random_date(date(1940, 1, 1), date(1970, 12, 31))

    def random_uom(self, dp_type: dp.DataPointType) -> uoms.UOM:
        uom = dp.dptype_uom_map[dp_type]
        idx = self.__class__.random_int(1, len(uom))
        return uom(idx)

    def random_dp_type(self) -> dp.DataPointType:
        return dp.DataPointType(self.__class__.random_int(1, self.dp_type_len))

    def random_frequency_type(self) -> dp.FrequencyType:
        return dp.FrequencyType(self.__class__.random_int(1, self.frequency_type_len))

    def random_weekdays(self, max_days) -> list[dp.WeekDay]:
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
        if 1 <= month <= 12:
            doms: list[int] = []
            while len(doms) <= max_dom:
                dom = self.random_int(1, BiometricsRandomData.max_dom_map[month])
                if dom not in doms:
                    doms.append(dom)
            return doms
        else:
            raise ValueError(f'Month must be 1 - 12 value {month:d}')



