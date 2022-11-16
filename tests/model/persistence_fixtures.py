import pytest

import biometrics_tracker.ipc.messages as msg
import biometrics_tracker.ipc.queue_manager as queue_manager


class QueueManagerPatch:
    """
    This class implements a monkey patch on the biometrics_tracker.ipc.Queues class and replaces the queue.Queue
    based dp_response and dp_request queues with lists.

    """
    def __init__(self):
        self.db_resp_queue: list[msg.MsgBase] = []
        self.completion_queue: list[msg.CompletionMsg] = []

    def receive_db_resp(self) -> msg.MsgBase:
        return self.db_resp_queue.pop()

    def send_db_resp(self, message: msg.MsgBase) -> None:
        self.db_resp_queue.insert(0, message)

    @pytest.fixture(autouse=True)
    def check_db_resp_queue_filter(self, monkeypatch):
        monkeypatch.setattr(queue_manager.Queues, 'check_db_resp_queue', self.receive_db_resp)

    @pytest.fixture(autouse=True)
    def send_db_resp_msg_filter(self, monkeypatch):
        monkeypatch.setattr(queue_manager.Queues, 'send_db_resp_msg', self.send_db_resp)
