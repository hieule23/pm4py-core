from pm4py.util import constants, exec_utils, xes_constants
from pm4py.streaming.algo.interface import StreamingAlgorithm
import logging
from copy import copy
from threading import Lock


class Parameters:
    CASE_ID_KEY = constants.PARAMETER_CONSTANT_CASEID_KEY
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY


class LogStreamingConformance(StreamingAlgorithm):
    def __init__(self, log, parameters=None):
        self.case_id_key = exec_utils.get_param_value(Parameters.CASE_ID_KEY, parameters, constants.CASE_CONCEPT_NAME)
        self.activity_key = exec_utils.get_param_value(Parameters.ACTIVITY_KEY, parameters,
                                                       xes_constants.DEFAULT_NAME_KEY)
        self.log = log
        self.last_act_dict = {}
        self.freq_act_dict = {}
        self.act_in_trace_dict = {}

    def _process(self, event):
        """
        Execution of the object when receive an event from live stream
        Parameters
        ----------
        event
            Event from live stream
        Returns
        -------
            Check all the conditions in log skeleton constraints
        """
        self.check(event)

    def check(self, event):
        """
        For each event received from live stream, check and update the 3 dictionaries
        Parameters
        ----------
        event

        Returns
        -------

        """
        case = event[self.case_id_key] if self.case_id_key in event else None
        activity = event[self.activity_key] if self.activity_key in event else None
        if case is not None and activity is not None:
            self.verify_directly_follow(event)
            self.verify_frequency(event)
            self.verify_always_before(event)
            self.verify_never_together(event)
        else:
            self.message_case_or_activity_not_in_event(event)

    def verify_directly_follow(self, event):
        pass

    def verify_frequency(self, event):
        pass

    def verify_always_before(self, event):
        pass

    def verify_never_together(self, event):
        pass

    def message_case_or_activity_not_in_event(self, event):
        """
        Sends a message if the case or the activity are not
        there in the event
        """
        logging.error("case or activities are none! " + str(event))


def apply(log, parameters=None):
    """
    Gets a log skeleton conformance checking object
    Parameters
    ----------
    log
        Log skeleton model object
    parameters
        Parameter of the algorithm
    Returns
    -------
        A log skeleton conformance checking object

    """
    if parameters == None:
        parameters = {}

    return LogStreamingConformance(log, parameters=parameters)
