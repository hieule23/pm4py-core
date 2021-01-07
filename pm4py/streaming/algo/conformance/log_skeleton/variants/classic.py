from pm4py.util import constants, exec_utils, xes_constants
from pm4py.streaming.algo.interface import StreamingAlgorithm
import logging
from pm4py.streaming.algo.conformance.log_skeleton.outputs import Outputs
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
        self.build_dictionary()
        StreamingAlgorithm.__init__(self)

    def build_dictionary(self):
        self.last_act_dict = {}
        self.freq_act_dict = {}
        self.act_in_trace_dict = {}
        self.deviations = {Outputs.DIRECTLY_FOLLOWS.value: [],
                           Outputs.ACTIV_FREQ.value: [],
                           Outputs.NEVER_TOGETHER.value: [],
                           Outputs.ALWAYS_BEFORE.value: []}

    def _process(self, event):
        """
        Execution of the object when receive an event from live stream
        Parameters
        ----------
        event
            Event from live stream
        Returns
        -------
            Check all the conditions in log_skeleton skeleton constraints
        """
        self.check(event)
        self.update_dict(event)

    def _current_result(self):
        """
        Return the current status of conformance checking.
        Returns
        -------
            Return the current status of conformance checking
        """
        return self.deviations

    def check(self, event):
        """
        For each event received from live stream, check 4 constraints
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

    def update_dict(self, event):
        """
        For each event received from live stream, update the 3 necessary dictionaries
        Parameters
        ----------
        event

        Returns
        -------
            3 updated dictionaries

        """
        case = event[self.case_id_key] if self.case_id_key in event else None
        activity = event[self.activity_key] if self.activity_key in event else None
        if case is not None and activity is not None:
            self.update_last_act_dict(event)
            self.update_freq_act_dict(event)
            self.update_act_in_trace_dict(event)

    def verify_directly_follow(self, event):
        case = event[self.case_id_key]
        activity = event[self.activity_key]
        if case in self.last_act_dict:
            act_1 = self.last_act_dict[case]
            act_2 = activity
            if (act_1, act_2) not in self.log[Outputs.DIRECTLY_FOLLOWS.value]:
                self.message_directly_follows_not_fit(event)
                error = (case, (act_1, act_2))
                self.deviations[Outputs.DIRECTLY_FOLLOWS.value].append(error)

    def verify_frequency(self, event):
        case = event[self.case_id_key]
        activity = event[self.activity_key]
        if case in self.freq_act_dict:
            if activity in self.freq_act_dict[case]:
                act_freq = self.freq_act_dict[case][activity] + 1
                if act_freq not in self.log[Outputs.ACTIV_FREQ.value]:
                    max_allow = max(self.log[Outputs.ACTIV_FREQ.value])
                    if act_freq > max_allow:
                        self.message_freq_act_not_fit(event)
                        error = (case, activity)
                        self.deviations[Outputs.ACTIV_FREQ.value].append(error)

    def verify_always_before(self, event):
        case = event[self.case_id_key]
        activity = event[self.activity_key]
        if case in self.act_in_trace_dict:
            must_occur = set([x[1] for x in self.log[Outputs.ALWAYS_BEFORE.value] if x[0] == activity])
            act_occurred = set(self.act_in_trace_dict[case])
            if not must_occur.issubset(act_occurred):

                self.message_always_before_not_fit(event)

    def verify_never_together(self, event):
        case = event[self.case_id_key]
        activity = event[self.activity_key]
        if case in self.act_in_trace_dict:
            not_occur = set([x[1] for x in self.log[Outputs.NEVER_TOGETHER.value] if x[0] == activity] +
                            [x[0] for x in self.log[Outputs.NEVER_TOGETHER.value] if x[1] == activity])
            act_occurred = set(self.act_in_trace_dict[case])
            if len(act_occurred.intersection(not_occur)) != 0:
                self.message_never_together_not_fit(event)

    def update_last_act_dict(self, event):
        case = event[self.case_id_key]
        activity = event[self.activity_key]
        self.act_in_trace_dict[case] = activity

    def update_freq_act_dict(self, event):
        case = event[self.case_id_key]
        activity = event[self.activity_key]
        if case not in self.freq_act_dict:
            self.freq_act_dict[case] = {activity: 1}
        else:
            if activity not in self.freq_act_dict[case]:
                self.freq_act_dict[case][activity] = 1
            else:
                self.freq_act_dict[case][activity] += 1

    def update_act_in_trace_dict(self, event):
        case = event[self.case_id_key]
        activity = event[self.activity_key]
        if case not in self.act_in_trace_dict:
            self.act_in_trace_dict[case] = [activity]
        else:
            self.act_in_trace_dict[case].append(activity)

    def message_case_or_activity_not_in_event(self, event):
        """
        Sends a message if the case or the activity are not
        there in the event
        """
        logging.error("case or activities are none! " + str(event))


def apply(log, parameters=None):
    """
    Gets a log_skeleton skeleton conformance checking object
    Parameters
    ----------
    log
        Log skeleton model object
    parameters
        Parameter of the algorithm
    Returns
    -------
        A log_skeleton skeleton conformance checking object

    """
    if parameters == None:
        parameters = {}

    return LogStreamingConformance(log, parameters=parameters)
