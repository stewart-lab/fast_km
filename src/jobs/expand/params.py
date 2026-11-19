from pydantic import BaseModel, Field
from src.fast_km_exception import FastKmException
from src.global_vars import MAX_CENSOR_YEAR, MIN_CENSOR_YEAR

MAX_PREVIEW_TERMS = 200

class ExpandJobParams(BaseModel):
    terms: list[str] =       Field(...,             description=f"Terms to expand, as they would be written in a KinderMiner job (at most {MAX_PREVIEW_TERMS})")
    censor_year_lower: int = Field(MIN_CENSOR_YEAR, description="Lower bound of publication year for the paper counts (inclusive)")
    censor_year_upper: int = Field(MAX_CENSOR_YEAR, description="Upper bound of publication year for the paper counts (inclusive)")
    counts: bool =           Field(True,            description="Whether to count the papers each variant matches. Without counts the job only lists the variants, which is faster.")
    id: str | None =         Field(None,            description="Optional job ID. If not provided, an ID will be generated.")

def validate_params(params: ExpandJobParams) -> None:
    params.censor_year_lower = max(MIN_CENSOR_YEAR, params.censor_year_lower)
    params.censor_year_upper = min(MAX_CENSOR_YEAR, params.censor_year_upper)
    if not params.terms:
        raise FastKmException('terms is required and must be a non-empty list')
    if len(params.terms) > MAX_PREVIEW_TERMS:
        raise FastKmException(f'terms cannot contain more than {MAX_PREVIEW_TERMS} terms')
    if params.censor_year_lower > params.censor_year_upper:
        raise FastKmException('censor_year_lower cannot be greater than censor_year_upper')
