import src.global_vars as gvars
from src.indexing.index import Index
from src.jobs.job_util import report_progress
from src.jobs.expand.params import ExpandJobParams, validate_params

def run_expand_job(params: ExpandJobParams) -> list[dict]:
    """Preview synonym expansion: for each term, the variants each of its subterms is searched as,
    and (optionally) how many papers the term and each variant match. Nothing is cached on disk."""
    validate_params(params)
    years = (params.censor_year_lower, params.censor_year_upper)
    idx = Index(gvars.data_dir, expand_synonyms=True)

    results = []
    for i, term in enumerate(params.terms):
        subterms = idx.expansion(term)
        result = {"term": term, "subterms": [
            {"subterm": subterm, "variants": [
                {"variant": v, "n": idx.count_phrase(v, *years) if params.counts else None} for v in variants
            ]} for subterm, variants in subterms
        ]}
        if params.counts:
            idx.expand_synonyms = False
            result["n_without_expansion"] = len(idx.search_documents(term, *years))
            idx.expand_synonyms = True
            result["n_with_expansion"] = len(idx.search_documents(term, *years))
            idx.delete_term_from_memory(term)
        results.append(result)
        report_progress(min((i + 1) / len(params.terms), 0.9999))

    idx.close()
    report_progress(1.0)
    return results
