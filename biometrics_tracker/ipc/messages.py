from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Iterator, Optional, Union

import schedule

import biometrics_tracker.model.datapoints as dp


class DBOperation(Enum):
    INSERT = auto()
    UPDATE = auto()
    DELETE_SINGLE = auto()
    DELETE_SET = auto()
    RETRIEVE_SINGLE = auto()
    RETRIEVE_SET = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields Weight.POUNDS, Weight.KILOS, etc
        :rtype: model.datapoints.DataPointType

        """
        for i in range(1, DBOperation.__len__()+1):
            yield DBOperation(i)


class Completion(Enum):
    SUCCESS = auto()
    FAILURE = auto()
    DB_CLOSED = auto()


@dataclass
class MsgBase:
    destination: Any
    replyto: Optional[Callable]


@dataclass
class PersonReqMsg(MsgBase):
    person_id: Optional[str]
    operation: DBOperation


@dataclass
class TrackingConfigReqMsg(MsgBase):
    person_id: str
    dp_type: Optional[dp.DataPointType]
    operation: DBOperation


@dataclass
class ScheduleEntryReqMsg(MsgBase):
    person_id: str
    seq_nbr: int
    last_triggered: Optional[datetime]
    operation: DBOperation


@dataclass
class DataPointReqMsg(MsgBase):
    person_id: str
    start: datetime
    end: datetime
    operation:  DBOperation
    dp_type: Optional[dp.DataPointType] = None


@dataclass
class DataPointRespMsg(MsgBase):
    person_id: str
    datapoints: list[dp.DataPoint]


@dataclass
class CreateDataBaseReqMsg(MsgBase):
    ...


@dataclass
class CloseDataBaseReqMsg(MsgBase):
    ...


@dataclass
class ObjectMsg(MsgBase):
    payload: Union[dp.Person, dp.TrackingConfig, dp.DataPoint]
    operation: DBOperation


@dataclass
class PersonMsg(ObjectMsg):
    payload: dp.Person


@dataclass
class TrackingConfigMsg(ObjectMsg):
    person_id: str
    payload: dp.TrackingConfig


@dataclass
class ScheduleEntryMsg(ObjectMsg):
    person_id: str
    seq_nbr: int
    payload: dp.ScheduleEntry


@dataclass
class ScheduleEntriesMsg(MsgBase):
    person_id: str
    entries: list[dp.ScheduleEntry]


@dataclass
class PeopleMsg(MsgBase):
    payload: list[tuple[str, str]]


@dataclass
class DataPointMsg(ObjectMsg):
    payload: dp.DataPoint
    key_timestamp: Optional[datetime] = field(default=None)


@dataclass
class CompletionMsg(MsgBase):
    status: Completion


@dataclass
class ImportExportCompletionMsg(CompletionMsg):
    messages: list[str]

    def msg_count(self) -> int:
        return len(self.messages)


@dataclass
class SchedulerCompletionMsg(CompletionMsg):
    jobs: list[schedule.Job]


completion_msg_type = Union[CompletionMsg, ImportExportCompletionMsg, SchedulerCompletionMsg]

db_req_msg_type = Union[PersonReqMsg, ScheduleEntryReqMsg, TrackingConfigReqMsg, DataPointReqMsg, PersonMsg,
                        ScheduleEntryMsg, DataPointMsg,
                        CloseDataBaseReqMsg, CreateDataBaseReqMsg]
db_resp_msg_type = Union[PersonMsg, ScheduleEntriesMsg, TrackingConfigMsg, PeopleMsg, DataPointMsg, DataPointRespMsg]

