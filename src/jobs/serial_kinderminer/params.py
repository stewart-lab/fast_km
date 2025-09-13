from src.jobs.kinderminer.params import KinderMinerJobParams
from src.jobs.kinderminer.params import validate_params as _validate_params

# SKiM uses same parameter model as kinderminer

def validate_params(params: KinderMinerJobParams) -> None:
    return _validate_params(params)  # reuse kinderminer validation