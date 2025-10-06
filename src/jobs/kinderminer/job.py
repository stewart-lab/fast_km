import src.global_vars as gvars
import src.kinderminer_algorithm as km
from src.jobs.job_util import report_progress
from src.indexing.index import Index
from src.jobs.kinderminer.params import KinderMinerJobParams, validate_params

def run_kinderminer_job(params: KinderMinerJobParams) -> list[dict]:
    validate_params(params)
    print("Running Kinderminer job...")

    if params.paired:
        return _run_paired_km(params)
    
    idx = Index(gvars.data_dir)

    idx.prep_for_search(params.a_terms + params.b_terms)
    results = []
    for b_i, b_term in enumerate(params.b_terms):
        for a_i, a_term in enumerate(params.a_terms):
            result = km.kinderminer_search(
                idx, 
                a_term, 
                b_term,
                censor_year_lower=params.censor_year_lower,
                censor_year_upper=params.censor_year_upper,
                return_pmids=params.return_pmids,
                top_n_articles_most_cited=params.top_n_articles_most_cited,
                top_n_articles_most_recent=params.top_n_articles_most_recent,
                top_n_articles_highest_impact_factor=params.top_n_articles_highest_impact_factor,
                scoring=params.scoring,
            )
            results.append(result)
            report_progress(_calculate_progress(b_i + 1, len(params.b_terms)))

        idx.delete_term_from_memory(b_term)

    results.sort(key=lambda ab: ab['ab_pred_score'], reverse=True)
    idx.close()

    report_progress(1.0)
    return results

def _run_paired_km(params: KinderMinerJobParams) -> list[dict]:
    idx = Index(gvars.data_dir)

    # track repeat terms to avoid deleting from cache too early
    repeat_terms = dict()
    for term in params.a_terms + params.b_terms:
        if term in repeat_terms:
            repeat_terms[term] += 1
        else:
            repeat_terms[term] = 1
    repeat_terms = {term: count for term, count in repeat_terms.items() if count > 1}
    
    idx.prep_for_search(params.a_terms + params.b_terms)
    results = []
    for i, a_term in enumerate(params.a_terms):
        b_term = params.b_terms[i]
        result = km.kinderminer_search(
            idx, 
            a_term, 
            b_term,
            censor_year_lower=params.censor_year_lower,
            censor_year_upper=params.censor_year_upper,
            return_pmids=params.return_pmids,
            top_n_articles_most_cited=params.top_n_articles_most_cited,
            top_n_articles_most_recent=params.top_n_articles_most_recent,
            top_n_articles_highest_impact_factor=params.top_n_articles_highest_impact_factor,
            scoring=params.scoring,
        )
        results.append(result)
        report_progress(_calculate_progress(i + 1, len(params.b_terms)))

        if a_term not in repeat_terms:
            idx.delete_term_from_memory(a_term)
        if b_term not in repeat_terms:
            idx.delete_term_from_memory(b_term)

    results.sort(key=lambda ab: ab['ab_pred_score'], reverse=True)

    idx.close()
    report_progress(1.0)
    return results

def _calculate_progress(b_complete: int, b_total: int) -> float:
    progress = b_complete / b_total
    progress = min(max(progress, 0.0), 0.9999)
    return progress