from enum import Enum

from pm4py.objects.dfg.retrieval.pandas import get_concurrent_events_dataframe
from pm4py.util import exec_utils, constants, xes_constants


class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    CASE_ID_KEY = constants.PARAMETER_CONSTANT_CASEID_KEY
    TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_TIMESTAMP_KEY
    START_TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_START_TIMESTAMP_KEY


def apply(dataframe, parameters=None):
    """
    Gets the number of times for which two activities have been concurrent in the log_skeleton

    Parameters
    --------------
    dataframe
        Pandas dataframe
    parameters
        Parameters of the algorithm, including:
        - Parameters.ACTIVITY_KEY => activity key
        - Parameters.CASE_ID_KEY => case id
        - Parameters.START_TIMESTAMP_KEY => start timestamp
        - Parameters.TIMESTAMP_KEY => complete timestamp

    Returns
    --------------
    ret_dict
        Dictionaries associating to a couple of activities (tuple) the number of times for which they have been
        executed in parallel in the log_skeleton
    """
    if parameters is None:
        parameters = {}

    activity_key = exec_utils.get_param_value(Parameters.ACTIVITY_KEY, parameters, xes_constants.DEFAULT_NAME_KEY)
    case_id_glue = exec_utils.get_param_value(Parameters.CASE_ID_KEY, parameters, constants.CASE_CONCEPT_NAME)
    timestamp_key = exec_utils.get_param_value(Parameters.TIMESTAMP_KEY, parameters,
                                               xes_constants.DEFAULT_TIMESTAMP_KEY)
    start_timestamp_key = exec_utils.get_param_value(Parameters.START_TIMESTAMP_KEY, parameters, None)

    concurrent_dataframe = get_concurrent_events_dataframe(dataframe, start_timestamp_key=start_timestamp_key,
                                                           timestamp_key=timestamp_key, case_id_glue=case_id_glue,
                                                           activity_key=activity_key)

    ret_dict = concurrent_dataframe.groupby([activity_key, activity_key + '_2']).size().to_dict()

    # assure to avoid problems with np.float64, by using the Python float type
    for el in ret_dict:
        ret_dict[el] = int(ret_dict[el])

    return ret_dict

