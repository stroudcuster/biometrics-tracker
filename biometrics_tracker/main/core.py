from abc import abstractmethod
from datetime import datetime, date, time
import pathlib
import sys
from typing import Callable, Optional

import biometrics_tracker.config.createconfig as config
import biometrics_tracker.gui.widgets as widgets
import biometrics_tracker.ipc.queue_manager as queues
import biometrics_tracker.ipc.messages as messages
import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.persistence as per


class CoreLogic:
    """
    This class is intended to be subclassed by classes that will add and/or update Datapoints
    """
    def __init__(self, config_info: config.ConfigInfo, queue_mgr: queues.Queues):
        """
        Creates an instance of biometrics_tracker.main.core.CoreLogic

        :param config_info: the application's configuration info
        :type config_info: model.importers.ConfigInfo
        :param queue_mgr: managers send and receiving messages using the database request, database response and
        completion queues.
        :type queue_mgr: biometrics_tracker.ipc.Queues
        """
        self.config_info: config.ConfigInfo = config_info
        """
        self.config_info.help_url = pathlib.Path(sys.path[-1:][0], 'biometrics_tracker', 'help',
                                                 'user-doc-index.html').as_uri()
        """

        self.queue_mgr = queue_mgr
        self.queue_sleep_millis = 250
        self.retrieved_person: Optional[dp.Person] = None
        self.check_completion_queue()
        self.check_response_queue()

    @abstractmethod
    def exit_app(self, msg: messages.CompletionMsg) -> None:
        """
        An abstract method that must be overridden to end the GUI event loop

        :param msg: a Completion message, usually a response to a Close Database request
        :return: None

        """
        ...

    def close_db(self):
        """
        Send a database request message to close the database and end the persistence thread

        :return: None

        """
        self.queue_mgr.send_db_req_msg(messages.CloseDataBaseReqMsg(destination=per.DataBase, replyto=self.exit_app))

    def check_completion_queue(self):
        """
        Check the completion queue, invoke the Callable in the message's destination property is it or the
        message object is not None

        :return: None

        """
        msg: messages.completion_msg_type = self.queue_mgr.check_completion_queue(block=False)
        if msg is not None and msg.destination is not None:
            msg.destination(msg)

    def check_response_queue(self):
        """
        Check the database response queue, invoke the Callable in the message's destination property is it or the
        message object is not None

        :return: None

        """
        msg: messages.db_resp_msg_type = self.queue_mgr.check_db_resp_queue(block=False)
        if msg is not None and msg.destination is not None:
            msg.destination(msg)

    def delete_datapoint(self, datapoint: dp.dptypes_union):
        """
        Send a database request message to delete a specified datapoint

        :param datapoint: the data point to be deleted
        :type datapoint: a subclass of biometrics_tracker.model.datapoint.DataPoint
        :return: None

        """
        self.queue_mgr.send_db_req_msg(
            messages.DataPointReqMsg(destination=per.DataBase,
                                     replyto=None, person_id=datapoint.person_id,
                                     start=datapoint.taken,
                                     end=datapoint.taken,
                                     dp_type=datapoint.type,
                                     operation=messages.DBOperation.DELETE_SINGLE))

    def save_datapoint(self, person: dp.Person, taken: datetime, widget: widgets.widget_union,
                       delete_if_zero: bool, db_operation: messages.DBOperation,
                       key_timestamp: Optional[datetime] = None):
        """
        Inserts or updates the database with datapoint property of the provided datapoint widget if
        the value fields(s) of the widget are non-zero.  If the value field(s) are zero and the
        delete_if_zero parameter is True, the datapoint is deleted from the database

        :param person: the person associated with the datapoint
        :type person: biometrics_tracker.model.datapoints.Person
        :param taken: the datetime associated withe datapoint
        :type taken: datetime.datetime
        :param widget: the widget containing the datapoint
        :type widget: biometrics_tracker.gui.widgets.widget_union
        :param delete_if_zero: if True delete the datapoint if the widget's value field(s) are zero
            This allows the user to delete an existing datapoint by changing it's value field(s) to zero.
        :type delete_if_zero: bool
        :param db_operation: controls whether the datapoint database row is inserted or updated
        :type db_operation: biometrics_tracker.ipc.messages.DBOperation
        :param key_timestamp: for DataPoint updates only: the Taken datetime prior to possible change by user
        :type key_timestamp: datetime
        :return: None

        """
        match widget.__class__.__name__:
            case widgets.BodyWeightWidget.__name__:
                if widget.get_value() == 0:
                    if delete_if_zero:
                        self.delete_datapoint(widget.datapoint)
                else:
                    body_weight = dp.BodyWeight(widget.get_value(), widget.uom)
                    body_weight_dp = dp.BodyWeightDP(person.id, taken, widget.get_note(),
                                                     body_weight, dp.DataPointType.BODY_WGT)
                    self.queue_mgr.send_db_req_msg(messages.DataPointMsg(destination=per.DataBase,
                                                                         replyto=None,
                                                                         payload=body_weight_dp,
                                                                         operation=db_operation,
                                                                         key_timestamp=key_timestamp))
            case widgets.BodyTempWidget.__name__:
                if widget.get_value() == 0:
                    if delete_if_zero:
                        self.delete_datapoint(widget.datapoint)
                else:
                    body_temp = dp.BodyTemperature(widget.get_value(), widget.uom)
                    body_temp_dp = dp.BodyTemperatureDP(person.id, taken, widget.get_note(),
                                                        body_temp, dp.DataPointType.BODY_TEMP)
                    self.queue_mgr.send_db_req_msg(messages.DataPointMsg(destination=per.DataBase,
                                                                         replyto=None,
                                                                         payload=body_temp_dp,
                                                                         operation=db_operation,
                                                                         key_timestamp=key_timestamp))
            case widgets.BloodGlucoseWidget.__name__:
                if widget.get_value() == 0:
                    if delete_if_zero:
                        self.delete_datapoint(widget.datapoint)
                else:
                    blood_glucose = dp.BloodGlucose(widget.get_value(), widget.uom)
                    blood_glucose_dp = dp.BloodGlucoseDP(person.id, taken, widget.get_note(),
                                                         blood_glucose, dp.DataPointType.BG)
                    self.queue_mgr.send_db_req_msg(messages.DataPointMsg(destination=per.DataBase,
                                                                         replyto=None,
                                                                         payload=blood_glucose_dp,
                                                                         operation=db_operation,
                                                                         key_timestamp=key_timestamp))
            case widgets.BloodPressureWidget.__name__:
                systolic, diastolic = widget.get_value()
                if widget.get_value() == (0, 0):
                    if delete_if_zero:
                        self.delete_datapoint(widget.datapoint)
                else:
                    blood_pressure = dp.BloodPressure(systolic, diastolic, widget.uom)
                    blood_pressure_dp = dp.BloodPressureDP(person.id, taken, widget.get_note(),
                                                           blood_pressure, dp.DataPointType.BP)
                    self.queue_mgr.send_db_req_msg(messages.DataPointMsg(destination=per.DataBase,
                                                                         replyto=None,
                                                                         payload=blood_pressure_dp,
                                                                         operation=db_operation,
                                                                         key_timestamp=key_timestamp))
            case widgets.PulseWidget.__name__:
                if widget.get_value() == 0:
                    if delete_if_zero:
                        self.delete_datapoint(widget.datapoint)
                else:
                    pulse = dp.Pulse(widget.get_value(), widget.uom)
                    pulse_dp = dp.PulseDP(person.id, taken, widget.get_note(),
                                          pulse, dp.DataPointType.PULSE)
                    self.queue_mgr.send_db_req_msg(messages.DataPointMsg(destination=per.DataBase,
                                                                         replyto=None,
                                                                         payload=pulse_dp,
                                                                         operation=db_operation,
                                                                         key_timestamp=key_timestamp))
            case _:
                ...

    def retrieve_person(self, person_id):
        def receive_person(msg: messages.PersonMsg):
            self.retrieved_person = msg.payload

        self.queue_mgr.send_db_req_msg(messages.PersonReqMsg(destination=per.DataBase, replyto=receive_person))
        self.queue_mgr.check_db_resp_queue(block=True)


