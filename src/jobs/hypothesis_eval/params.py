from pydantic import BaseModel, Field
from src.fast_km_exception import FastKmException

class HypothesisEvalJobParams(BaseModel):
    data: list[dict] = Field(..., description="KM/SKiM results to use to evaluate the hypotheses.")
    KM_hypothesis: str | None = Field(None, description="Hypothesis to evaluate using KM results.")
    SKIM_hypotheses: dict | None = Field(None, description="Hypotheses to evaluate using SKiM results.")
    KM_direct_comp_hypothesis: str | None = Field(None, description="Hypothesis to use for direct comparison.")
    model: str = Field("o3-mini", description="LLM to use to evaluate the hypotheses.")
    top_n_articles_most_cited: int = Field(5, description="Number of most cited articles to include in the context provided to the LLM.")
    top_n_articles_most_recent: int = Field(5, description="Number of most recent articles to include in the context provided to the LLM.")
    post_n: int = Field(5, description=".")
    id: str | None = Field(None, description="Optional job ID. If not provided, an ID will be generated.")

def validate_params(params: HypothesisEvalJobParams) -> None:
    data = params.data

    if not data or len(data) == 0:
        raise FastKmException('data is required and must be a non-empty list')
    
    # determine if KM or SKiM or direct comparison KM
    count_with_c = len([x for x in data if 'c_term' in x])
    if count_with_c != 0 and count_with_c != len(data):
        raise FastKmException('data must be all KM or all SKiM')
    
    is_km = params.KM_hypothesis is not None
    is_skim = params.SKIM_hypotheses is not None
    is_km_direct_comparison = params.KM_direct_comp_hypothesis is not None

    if not (is_km or is_skim or is_km_direct_comparison):
        raise FastKmException('job must be KM, SKiM, or direct comparison KM')

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
    if is_km_direct_comparison:
        if not isinstance(params.KM_direct_comp_hypothesis, str):
            raise FastKmException('KM_direct_comp_hypothesis must be a string')
        if '{a_term}' not in params.KM_direct_comp_hypothesis or '{b_term1}' not in params.KM_direct_comp_hypothesis or '{b_term2}' not in params.KM_direct_comp_hypothesis:
            raise FastKmException('KM_direct_comp_hypothesis must contain {a_term}, {b_term1}, and {b_term2}')
    elif is_km:
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