from enum import Enum
from pm4py.util import exec_utils
from pm4py.streaming.algo.conformance.log_skeleton.variants import classic


class Variants(Enum):
    CLASSIC = classic


def apply(model, variant=Variants.CLASSIC, parameters=None):

    return exec_utils.get_variant(variant).apply(model, parameters=parameters)
