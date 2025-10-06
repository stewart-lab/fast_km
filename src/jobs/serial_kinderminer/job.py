import src.global_vars as gvars
import src.kinderminer_algorithm as km
from src.jobs.job_util import report_progress
from src.jobs.serial_kinderminer.params import validate_params
from src.indexing.index import Index
from src.jobs.kinderminer.params import KinderMinerJobParams

def run_serial_kinderminer_job(params: KinderMinerJobParams) -> list[dict]:
    validate_params(params)
    print("Running Serial Kinderminer job...")

    if params.paired:
        return _run_paired_skim(params)

    top_n_ab_padding = 20 if params.valid_bc_hit_pval < 1.0 else 0
    idx = Index(gvars.data_dir)
    
    # test AB pairs
    idx.prep_for_search(params.a_terms + params.b_terms)
    ab_results = []
    for b_i, b_term in enumerate(params.b_terms):
        for a_i, a_term in enumerate(params.a_terms):
            ab_result = km.kinderminer_search(
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

            if ab_result['ab_pvalue'] <= params.ab_fet_threshold:
                ab_results.append(ab_result)
            
            progress = _calculate_progress(a_i + 1, len(params.a_terms), b_i + 1, len(params.b_terms), 0, len(params.c_terms), params.top_n_ab + top_n_ab_padding)
            report_progress(progress)

        idx.delete_term_from_memory(b_term)

    # take top N AB results
    ab_results.sort(key=lambda ab: ab['ab_pred_score'], reverse=True)
    top_ab_results = ab_results[:params.top_n_ab + top_n_ab_padding]

    # test BC and AC pairs for each top AB result
    idx.prep_for_search(params.c_terms)
    abc_results = []
    for c_i, c_term in enumerate(params.c_terms):
        for ab_result in top_ab_results:
            a_term = ab_result['a_term']
            b_term = ab_result['b_term']

            abc_result = dict()
            bc_result = km.kinderminer_search(
                idx, 
                None, 
                b_term, 
                c_term,
                censor_year_lower=params.censor_year_lower,
                censor_year_upper=params.censor_year_upper,
                return_pmids=params.return_pmids,
                top_n_articles_most_cited=params.top_n_articles_most_cited,
                top_n_articles_most_recent=params.top_n_articles_most_recent,
                top_n_articles_highest_impact_factor=params.top_n_articles_highest_impact_factor,
                scoring=params.scoring,
            )

            ac_result = km.kinderminer_search(
                idx, 
                a_term, 
                None, 
                c_term,
                censor_year_lower=params.censor_year_lower,
                censor_year_upper=params.censor_year_upper,
                return_pmids=params.return_pmids,
                top_n_articles_most_cited=params.top_n_articles_most_cited,
                top_n_articles_most_recent=params.top_n_articles_most_recent,
                top_n_articles_highest_impact_factor=params.top_n_articles_highest_impact_factor,
                scoring=params.scoring,
            )

            abc_result.update(ab_result)
            abc_result.update(bc_result)
            abc_result.update(ac_result)

            if abc_result['bc_pvalue'] <= params.bc_fet_threshold:
                abc_results.append(abc_result)

            progress = _calculate_progress(len(params.a_terms), len(params.a_terms), len(params.b_terms), len(params.b_terms), c_i + 1, len(params.c_terms), len(top_ab_results))
            report_progress(progress)

        idx.delete_term_from_memory(c_term)

    # sort by bc_prediction_score descending
    abc_results.sort(key=lambda abc: abc['bc_pred_score'], reverse=True)

    if top_n_ab_padding > 0:
        # sometimes high prediction score A-B pairs with no valid B-C pairs will 
        # crowd out lower-scoring A-B pairs that have valid B-C pairs. we want to
        # ignore the former in favor of including the latter. we added 
        # extra B-terms above as padding, now we need to filter out any
        # extra B-terms not in the top N.

        # Bs including padding in prediction score order, including those without B-C hits
        ranked_bs = [ab['b_term'] for ab in ab_results]

        # Bs with valid B-C hits
        valid_bs = set([abc['b_term'] for abc in abc_results if abc['bc_pvalue'] <= params.valid_bc_hit_pval])

        # top N Bs with valid B-C hits
        ranked_top_n_valid_bs = set([b for b in ranked_bs if b in valid_bs][:params.top_n_ab])

        # filter the ABC results
        abc_results = [abc for abc in abc_results if abc['b_term'] in ranked_top_n_valid_bs]

    idx.close()
    report_progress(1.0)
    return abc_results

def _run_paired_skim(params: KinderMinerJobParams) -> list[dict]:
    idx = Index(gvars.data_dir)
    
    # track repeat terms to avoid deleting from cache too early
    repeat_terms = dict()
    for term in params.a_terms + params.b_terms + params.c_terms:
        if term in repeat_terms:
            repeat_terms[term] += 1
        else:
            repeat_terms[term] = 1
    repeat_terms = {term: count for term, count in repeat_terms.items() if count > 1}

    abc_results = []
    idx.prep_for_search(params.a_terms + params.b_terms + params.c_terms)
    for i, a_term in enumerate(params.a_terms):
        b_term = params.b_terms[i]
        c_term = params.c_terms[i]
        result = km.kinderminer_search(
            idx, 
            a_term, 
            b_term, 
            c_term,
            censor_year_lower=params.censor_year_lower,
            censor_year_upper=params.censor_year_upper,
            return_pmids=params.return_pmids,
            top_n_articles_most_cited=params.top_n_articles_most_cited,
            top_n_articles_most_recent=params.top_n_articles_most_recent,
            top_n_articles_highest_impact_factor=params.top_n_articles_highest_impact_factor,
            scoring=params.scoring,
        )

        if result['ab_pvalue'] <= params.ab_fet_threshold and result['bc_pvalue'] <= params.bc_fet_threshold:
            abc_results.append(result)

        report_progress(i + 1, len(params.a_terms), 0, 1, 0, 0, 0)
        
        if a_term not in repeat_terms:
            idx.delete_term_from_memory(a_term)
        if b_term not in repeat_terms:
            idx.delete_term_from_memory(b_term)
        if c_term not in repeat_terms:
            idx.delete_term_from_memory(c_term)

    # sort by bc_prediction_score descending
    abc_results.sort(key=lambda abc: abc['bc_pred_score'], reverse=True)

    idx.close()
    report_progress(1.0)
    return abc_results

def _calculate_progress(a_complete: int, a_total: int, b_complete: int, b_total: int, c_complete: int, c_total: int, n_ab: int) -> float:
    ab_done = b_complete
    bc_done = c_complete * n_ab
    done = ab_done + bc_done
    
    ab_total = a_total * b_total
    bc_total = c_total * n_ab
    total = ab_total + bc_total

    progress = done / total
    progress = min(max(progress, 0.0), 0.9999)

    # DEBUG: save to .csv file
    # import time
    # with open("_progress_log.csv", "a") as f:
    #     current_time = time.perf_counter()
    #     f.write(f"{current_time},{progress}\n")

    return progress