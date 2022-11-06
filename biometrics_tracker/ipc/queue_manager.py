import queue
import time
from typing import Optional

import biometrics_tracker.ipc.messages as messages


class Queues:
    """
    Acts as a container for a set of queues used to implement interprocess communication between a multi-threaded
    application's GUI front end and it's persistence manager and other long-running processes such as report
    generation.  There are three queues: request and response queues for passing requests and objects between
    the GUI front end and the persistance manager and a completion queue for mesages that indicate that a
    long-running process has completed.
    """
    def __init__(self, sleep_seconds: float):
        self.sleep_seconds = sleep_seconds
        self.db_request_queue = queue.Queue()
        self.db_response_queue = queue.Queue()
        self.completion_queue = queue.Queue()
        self.timeout_sec = 10

    def send_db_req_msg(self, msg: messages.db_req_msg_type):
        self.db_request_queue.put(msg)

    def check_db_req_queue(self, block: bool) -> Optional[messages.db_req_msg_type]:

        try:
            msg: Optional[messages.db_req_msg_type] = None
            msg = self.db_request_queue.get(block=block, timeout=self.timeout_sec)
            return msg
        except queue.Empty:
            return

    def send_db_resp_msg(self, msg: messages.db_resp_msg_type):
        self.db_response_queue.put(msg)

    def check_db_resp_queue(self, block: bool) -> Optional[messages.db_resp_msg_type]:
        try:
            msg: Optional[messages.db_resp_msg_type] = None
            msg = self.db_response_queue.get(block=block, timeout=self.timeout_sec)
            return msg
        except queue.Empty:
            return

    def send_completion_msg(self, msg: messages.completion_msg_type):
        self.completion_queue.put(msg)

    def check_completion_queue(self, block: bool) -> Optional[messages.CompletionMsg]:
        try:
            msg: Optional[messages.CompletionMsg] = None
            msg = self.completion_queue.get(block=block, timeout=self.timeout_sec)
            return msg
        except queue.Empty:
            return
