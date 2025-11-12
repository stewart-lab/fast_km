from typing import Literal
from pydantic import BaseModel, ConfigDict, Field
from src.fast_km_exception import FastKmException
from src.global_vars import MAX_CENSOR_YEAR, MIN_CENSOR_YEAR

class KinderMinerJobParams(BaseModel):
    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,    # accept (deprecated) aliases
        serialize_by_alias=True,   # convert any deprecated aliases to the current names
    )

    a_terms: list[str] =                        Field(...,                                     description="List of A-terms")
    b_terms: list[str] =                        Field(...,                                     description="List of B-terms")
    c_terms: list[str] | None =                 Field(None,                                    description="List of C-terms; if provided, SKiM will be run instead of KM")
    censor_year_lower: int =                    Field(MIN_CENSOR_YEAR,                         description="Lower bound of publication year for article censoring (inclusive)")
    censor_year_upper: int =                    Field(MAX_CENSOR_YEAR, alias="censor_year",    description="Upper bound of publication year for article censoring (inclusive)")
    scoring: Literal["fet", "chi-square"] =     Field("fet",                                   description="Scoring method to use: 'fet' (Fisher's Exact Test, default) or 'chi-square' (Chi-squared test)")
    return_pmids: bool =                        Field(False,                                   description="Whether to return PMIDs of articles containing the terms")
    top_n_articles_most_cited: int =            Field(0,               alias="top_n_articles", description="Number of most cited articles to report in the PMID results. If 'return_pmids' is False, this parameter is ignored.")
    top_n_articles_most_recent: int =           Field(10,                                      description="Number of most recent articles to report in the PMID results. If 'return_pmids' is False, this parameter is ignored.")
    top_n_articles_highest_impact_factor: int = Field(0,                                       description="Number of articles with the highest impact factor (citations divided by years since publication) to report in the PMID results. If 'return_pmids' is False, this parameter is ignored.")
    query_knowledge_graph: bool =               Field(False,                                   description="Whether to query the knowledge graph for relationships between terms.")
    rel_pvalue_cutoff: float =                  Field(1e-5,                                    description="KM/SKiM hits above this p-value will not be used for knowledge graph queries. Ignored if 'query_knowledge_graph' is False.")
    top_n_ab: int =                             Field(50,              alias="top_n",          description="(Ignored for KM) Number of top AB pairs to use for BC testing.")
    ab_fet_threshold: float =                   Field(1e-5,                                    description="(Ignored for KM) Maximum AB p-value to consider a hit.")
    bc_fet_threshold: float =                   Field(0.9999,                                  description="(Ignored for KM) Maximum BC p-value to consider a hit.")
    valid_bc_hit_pval: float =                  Field(1.0,                                     description="(Ignored for KM) If this value is <1, SKiM will try to replace AB hits that have no valid BC hit with an alternative AB. This parameter defines what constitutes a valid BC hit.")
    paired: bool =                              Field(False,                                   description="Whether to pair the ABC terms by location in their respective lists. If True, len(a_terms) and len(b_terms) must be equal. If C-terms are provided, len(c_terms) must be equal to those as well. If False (default), all combinations of A, B, and C terms will be tested.")
    id: str | None =                            Field(None,                                    description="Optional job ID. If not provided, an ID will be generated.")

def validate_params(params: KinderMinerJobParams) -> None:
    # handle non-fatal errors / warnings
    params.censor_year_lower = max(MIN_CENSOR_YEAR, params.censor_year_lower)
    params.censor_year_upper = min(MAX_CENSOR_YEAR, params.censor_year_upper)

    # handle fatal errors
    if not params.a_terms:
        raise FastKmException('a_terms is required and must be a non-empty list')
    if not params.b_terms:
        raise FastKmException('b_terms is required and must be a non-empty list')
    if params.censor_year_lower > params.censor_year_upper:
        raise FastKmException('censor_year_lower cannot be greater than censor_year_upper')
    if len(params.a_terms) > 100:
        raise FastKmException('a_terms cannot contain more than 100 terms')
    
    if params.paired:
        if len(params.a_terms) != len(params.b_terms):
            raise FastKmException('For paired KM/SKiM, a_terms and b_terms must have the same length')
        if params.c_terms and (len(params.a_terms) != len(params.c_terms)):
            raise FastKmException('For paired SKiM, a_terms, b_terms, and c_terms must have the same length')
        
    # SKiM-specific checks
    if params.c_terms:
        if len(params.a_terms) > 1:
            raise FastKmException('For SKiM, only one A-term is allowed')
        if not params.top_n_ab or params.top_n_ab <= 0:
            raise FastKmException('For SKiM, top_n_ab must be a positive integer')