from pydantic import BaseModel, Field
from src.fast_km_exception import FastKmException
from src.global_vars import MAX_CENSOR_YEAR, MIN_CENSOR_YEAR

class HypothesisEvalJobParams(BaseModel):
    data: list[dict] =                Field(...,                                     description="KM/SKiM results to use to evaluate the hypotheses.")
    KM_hypothesis: str | None =       Field(None,                                    description="Hypothesis to evaluate using KM results.")
    SKIM_hypotheses: dict | None =    Field(None,                                    description="Hypotheses to evaluate using SKiM results.")
    is_dch: bool =                    Field(False,                                   description="If True, run as a Direct Comparison Hypothesis job. Requires exactly 2 data entries with the same A-term, and KM_hypothesis.")
    model: str =                      Field("o3-mini",                               description="LLM to use to evaluate the hypotheses.")
    top_n_articles_most_cited: int =  Field(5,                                       description="Number of most cited articles to include in the context provided to the LLM.")
    top_n_articles_most_recent: int = Field(5,                                       description="Number of most recent articles to include in the context provided to the LLM.")
    post_n: int =                     Field(5,                                       description=".")
    censor_year_lower: int =          Field(MIN_CENSOR_YEAR,                         description="Lower bound of publication year for article censoring (inclusive). Ignored if a PMID list is supplied.")
    censor_year_upper: int =          Field(MAX_CENSOR_YEAR,   alias="censor_year",  description="Upper bound of publication year for article censoring (inclusive). Ignored if a PMID list is supplied.")
    id: str | None =                  Field(None,                                    description="Optional job ID. If not provided, an ID will be generated.")

def validate_params(params: HypothesisEvalJobParams) -> None:
    data = params.data

    if not data or len(data) == 0:
        raise FastKmException('data is required and must be a non-empty list')
    
    # DCH-specific validation
    if params.is_dch:
        if len(data) != 2:
            raise FastKmException('DCH requires exactly 2 data entries')
        if params.KM_hypothesis is None:
            raise FastKmException('DCH requires KM_hypothesis')
        for item in data:
            if 'a_term' not in item:
                raise FastKmException('a_term is required')
            if 'b_term' not in item:
                raise FastKmException('b_term is required')
        a_terms = {item['a_term'] for item in data}
        b_terms = {item['b_term'] for item in data}
        if len(a_terms) != 1 and len(b_terms) != 1:
            raise FastKmException('DCH requires the two entries to share either an a_term or a b_term')
        if '{a_term}' not in params.KM_hypothesis or '{b_term}' not in params.KM_hypothesis:
            raise FastKmException('KM_hypothesis must contain {a_term} and {b_term}')
        return

    # determine if KM or SKiM or direct comparison KM
    count_with_c = len([x for x in data if 'c_term' in x])
    if count_with_c != 0 and count_with_c != len(data):
        raise FastKmException('data must be all KM or all SKiM')
    
    is_km = params.KM_hypothesis is not None
    is_skim = params.SKIM_hypotheses is not None

    if not (is_km or is_skim):
        raise FastKmException('job must be KM or SKiM')

    # validate data
    for item in data:
        if 'a_term' not in item:
            raise FastKmException('a_term is required')
        if 'b_term' not in item:
            raise FastKmException('b_term is required')
        if is_skim and 'c_term' not in item:
            raise FastKmException('c_term is required for SKiM')
        
        if 'ab_pmid_intersection' not in item or len(item['ab_pmid_intersection']) == 0:
            raise FastKmException('ab_pmid_intersection is required')
        if is_skim and ('bc_pmid_intersection' not in item or len(item['bc_pmid_intersection'])) == 0:
            raise FastKmException('bc_pmid_intersection is required for SKiM')
    
    # validate hypotheses
    if is_km:
        if not isinstance(params.KM_hypothesis, str):
            raise FastKmException('KM_hypothesis must be a string')
        if '{a_term}' not in params.KM_hypothesis or '{b_term}' not in params.KM_hypothesis:
            raise FastKmException('KM_hypothesis must contain {a_term} and {b_term}')
    elif is_skim:
        if not isinstance(params.SKIM_hypotheses, dict):
            raise FastKmException('SKIM_hypotheses must be a dictionary')
        if 'AB' not in params.SKIM_hypotheses:
            raise FastKmException('AB hypothesis is required for SKiM')
        if 'BC' not in params.SKIM_hypotheses:
            raise FastKmException('BC hypothesis is required for SKiM')
        if 'rel_AC' not in params.SKIM_hypotheses:
            raise FastKmException('rel_AC hypothesis is required for SKiM')
        if 'AC' not in params.SKIM_hypotheses:
            raise FastKmException('AC hypothesis is required for SKiM')
        if 'ABC' not in params.SKIM_hypotheses:
            raise FastKmException('ABC hypothesis is required for SKiM')
        
        if '{a_term}' not in params.SKIM_hypotheses['AB'] or '{b_term}' not in params.SKIM_hypotheses['AB']:
            raise FastKmException('AB hypothesis must contain {a_term} and {b_term}')
        if '{b_term}' not in params.SKIM_hypotheses['BC'] or '{c_term}' not in params.SKIM_hypotheses['BC']:
            raise FastKmException('BC hypothesis must contain {b_term} and {c_term}')
        if '{a_term}' not in params.SKIM_hypotheses['AC'] or '{c_term}' not in params.SKIM_hypotheses['AC']:
            raise FastKmException('AC hypothesis must contain {a_term} and {c_term}')
        if '{a_term}' not in params.SKIM_hypotheses['rel_AC'] or '{c_term}' not in params.SKIM_hypotheses['rel_AC']:
            raise FastKmException('rel_AC hypothesis must contain {a_term} and {c_term}')
        if '{a_term}' not in params.SKIM_hypotheses['ABC'] or '{b_term}' not in params.SKIM_hypotheses['ABC'] or '{c_term}' not in params.SKIM_hypotheses['ABC']:
            raise FastKmException('ABC hypothesis must contain {a_term}, {b_term}, and {c_term}')