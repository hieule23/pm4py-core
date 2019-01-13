from copy import deepcopy

import numpy as np

from pm4py.algo.filtering.tracelog.attributes import attributes_filter
from pm4py.objects.log.log import TraceLog, Trace, Event

try:
    from Lib.statistics import median
except:
    from statistics import median


def form_log_from_dictio_couple(first_cases_repr, second_cases_repr):
    """
    Form a log from a couple of dictionary, to use for
    root cause analysis

    Parameters
    -------------
    first_cases_repr
        First cases representation
    second_cases_repr
        Second cases representation

    Returns
    ------------
    log
        Trace log object
    """
    log = TraceLog()

    multiplier_first = int(max(float(len(second_cases_repr)) / float(len(first_cases_repr)), 1))
    multiplier_second = int(max(float(len(first_cases_repr)) / float(len(second_cases_repr)), 1))

    for j in range(multiplier_first):
        for i in range(len(first_cases_repr)):
            trace = Trace()
            event = Event(first_cases_repr[i])
            trace.append(event)
            log.append(trace)

    for j in range(multiplier_second):
        for i in range(len(second_cases_repr)):
            trace = Trace()
            event = Event(second_cases_repr[i])
            trace.append(event)
            log.append(trace)

    return log


def form_representation_from_dictio_couple(first_cases_repr, second_cases_repr, string_attributes, numeric_attributes):
    """
    Gets a log representation, useful for training the decision tree,
    from a couple of dictionaries along with the list of string attributes
    and numeric attributes to consider, to use for root cause analysis

    Parameters
    ------------
    first_cases_repr
        First cases representation
    second_cases_repr
        Second cases representation
    string_attributes
        String attributes contained in the log
    numeric_attributes
        Numeric attributes contained in the log

    Returns
    ------------
    data
        Matrix representation of the event log
    feature_names
        Array of feature names
    """
    from pm4py.algo.other.decisiontree import get_log_representation

    log = form_log_from_dictio_couple(first_cases_repr, second_cases_repr)

    data, feature_names = get_log_representation.get_representation(log, [], string_attributes, [], numeric_attributes)

    return data, feature_names


def diagnose_from_trans_fitness(log, trans_fitness, parameters=None):
    """
    Perform root cause analysis starting from transition fitness knowledge

    Parameters
    -------------
    log
        Trace log object
    trans_fitness
        Transition fitness object
    parameters
        Possible parameters of the algorithm, including:
            string_attributes -> List of string event attributes to consider
                in building the decision tree
            numeric_attributes -> List of numeric event attributes to consider
                in building the decision tree

    Returns
    -----------
    diagnostics
        For each problematic transition:
            - a decision tree comparing fit and unfit executions
            - feature names
            - classes
    """
    if parameters is None:
        parameters = {}

    from pm4py.algo.other.decisiontree import mine_decision_tree

    diagnostics = {}
    string_attributes = parameters["string_attributes"] if "string_attributes" in parameters else []
    numeric_attributes = parameters["numeric_attributes"] if "numeric_attributes" in parameters else []

    for trans in trans_fitness:
        if len(trans_fitness[trans]["underfed_traces"]) > 0:
            fit_cases_repr = []
            underfed_cases_repr = []

            for trace in log:
                if trace in trans_fitness[trans]["underfed_traces"]:
                    underfed_cases_repr.append(trans_fitness[trans]["underfed_traces"][trace][0])
                elif trace in trans_fitness[trans]["fit_traces"]:
                    fit_cases_repr.append(trans_fitness[trans]["fit_traces"][trace][0])

            if fit_cases_repr and underfed_cases_repr:
                data, feature_names = form_representation_from_dictio_couple(fit_cases_repr, underfed_cases_repr,
                                                                             string_attributes, numeric_attributes)
                target = []
                classes = []

                multiplier_first = int(max(float(len(underfed_cases_repr)) / float(len(fit_cases_repr)), 1))
                multiplier_second = int(max(float(len(fit_cases_repr)) / float(len(underfed_cases_repr)), 1))

                for j in range(multiplier_first):
                    for i in range(len(fit_cases_repr)):
                        target.append(0)
                classes.append("fit")

                for j in range(multiplier_second):
                    for i in range(len(underfed_cases_repr)):
                        target.append(1)
                classes.append("underfed")

                target = np.asarray(target)
                clf = mine_decision_tree.mine(data, target)
                diagn_dict = {"clf": clf, "data": data, "feature_names": feature_names, "target": target,
                              "classes": classes}

                diagnostics[trans] = diagn_dict

    return diagnostics
