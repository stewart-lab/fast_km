from enum import Enum

class JobType(Enum):
    KINDERMINER = "kinderminer"
    SERIAL_KINDERMINER = "serial_kinderminer"
    HYPOTHESIS_EVAL = "hypothesis_eval"
    UPDATE_INDEX = "update_index"