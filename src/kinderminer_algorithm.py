import math
import scipy.stats
from src.global_vars import MAX_CENSOR_YEAR, MIN_CENSOR_YEAR
from src.indexing.index import Index

def kinderminer_search(
        idx: Index, 
        a_term: str, 
        b_term: str, 
        c_term: str = None, 
        censor_year_lower: int = MIN_CENSOR_YEAR,
        censor_year_upper: int = MAX_CENSOR_YEAR, 
        return_pmids: bool = False, 
        top_n_articles_most_cited: int = 0,
        top_n_articles_most_recent: int = 10, 
        top_n_articles_highest_impact_factor: int = 0,
        scoring: str = 'fet'
    ) -> dict:
    """"""
    # validate input
    if not ((a_term and b_term) or (b_term and c_term) or (a_term and c_term)):
        raise ValueError("Must provide at least two of the three terms (A, B, C)")

    result = dict()

    # search documents in the corpus for the ABC terms
    n_total_abstracts = idx.count_documents(censor_year_lower, censor_year_upper)
    result['n_articles'] = n_total_abstracts
    if a_term and b_term:
        a_term_set = idx.search_documents(a_term, censor_year_lower, censor_year_upper)
        b_term_set = idx.search_documents(b_term, censor_year_lower, censor_year_upper)
        ab_table = _get_contingency_table(a_term_set, b_term_set, n_total_abstracts)
    if b_term and c_term:
        b_term_set = idx.search_documents(b_term, censor_year_lower, censor_year_upper)
        c_term_set = idx.search_documents(c_term, censor_year_lower, censor_year_upper)
        bc_table = _get_contingency_table(b_term_set, c_term_set, n_total_abstracts)
    if a_term and c_term:
        a_term_set = idx.search_documents(a_term, censor_year_lower, censor_year_upper)
        c_term_set = idx.search_documents(c_term, censor_year_lower, censor_year_upper)
        ac_table = _get_contingency_table(a_term_set, c_term_set, n_total_abstracts)
    
    # statistical testing and scoring
    if a_term and b_term:
        ab_pvalue = _compute_pvalue(ab_table, scoring)
        ab_sort_ratio = _compute_sort_ratio(ab_table)
        ab_prediction_score = _compute_prediction_score(ab_pvalue, ab_sort_ratio)
        result['ab_sort_ratio'] = ab_sort_ratio
        result['ab_prediction_score'] = ab_prediction_score
        result['ab_pvalue'] = ab_pvalue
    if b_term and c_term:
        bc_pvalue = _compute_pvalue(bc_table, scoring)
        bc_sort_ratio = _compute_sort_ratio(bc_table)
        bc_prediction_score = _compute_prediction_score(bc_pvalue, bc_sort_ratio)
        result['bc_sort_ratio'] = bc_sort_ratio
        result['bc_prediction_score'] = bc_prediction_score
        result['bc_pvalue'] = bc_pvalue
    if a_term and c_term:
        ac_pvalue = _compute_pvalue(ac_table, scoring)
        ac_sort_ratio = _compute_sort_ratio(ac_table)
        ac_prediction_score = _compute_prediction_score(ac_pvalue, ac_sort_ratio)
        result['ac_sort_ratio'] = ac_sort_ratio
        result['ac_prediction_score'] = ac_prediction_score
        result['ac_pvalue'] = ac_pvalue

    # get information about the sets
    if a_term:
        result['a_term'] = a_term
        result['len(a_term_set)'] = len(a_term_set)
    if b_term:
        result['b_term'] = b_term
        result['len(b_term_set)'] = len(b_term_set)
    if c_term:
        result['c_term'] = c_term
        result['len(c_term_set)'] = len(c_term_set)

    if a_term and b_term:
        result['len(a_b_intersect)'] = ab_table[0][0]
    if b_term and c_term:
        result['len(b_c_intersect)'] = bc_table[0][0]
    if a_term and c_term:
        result['len(a_c_intersect)'] = ac_table[0][0]

    # report PMID intersections
    if return_pmids:
        if a_term and b_term:
            ab_pmid_intersection = a_term_set & b_term_set
            top_n_ab_pmids = set()
            top_n_ab_pmids.update(idx.top_n_pmids_by_year(ab_pmid_intersection, top_n_articles_most_recent))
            top_n_ab_pmids.update(idx.top_n_pmids_by_impact_factor(ab_pmid_intersection, top_n_articles_highest_impact_factor))
            top_n_ab_pmids.update(idx.top_n_pmids_by_citation_count(ab_pmid_intersection, top_n_articles_most_cited))
            result['ab_pmid_intersection'] = list(top_n_ab_pmids)
        if b_term and c_term:
            bc_pmid_intersection = b_term_set & c_term_set
            top_n_bc_pmids = set()
            top_n_bc_pmids.update(idx.top_n_pmids_by_year(bc_pmid_intersection, top_n_articles_most_recent))
            top_n_bc_pmids.update(idx.top_n_pmids_by_impact_factor(bc_pmid_intersection, top_n_articles_highest_impact_factor))
            top_n_bc_pmids.update(idx.top_n_pmids_by_citation_count(bc_pmid_intersection, top_n_articles_most_cited))
            result['bc_pmid_intersection'] = list(top_n_bc_pmids)
        if a_term and c_term:
            ac_pmid_intersection = a_term_set & c_term_set
            top_n_ac_pmids = set()
            top_n_ac_pmids.update(idx.top_n_pmids_by_year(ac_pmid_intersection, top_n_articles_most_recent))
            top_n_ac_pmids.update(idx.top_n_pmids_by_impact_factor(ac_pmid_intersection, top_n_articles_highest_impact_factor))
            top_n_ac_pmids.update(idx.top_n_pmids_by_citation_count(ac_pmid_intersection, top_n_articles_most_cited))
            result['ac_pmid_intersection'] = list(top_n_ac_pmids)

    return result

def _get_contingency_table(a_term_set: set, b_term_set: set, total_n: int) -> list[list[int]]:
    """Populates the table for the Fisher's exact test"""
    a_and_b = len(a_term_set & b_term_set)
    b_not_a = len(b_term_set) - a_and_b
    a_not_b = len(a_term_set) - a_and_b
    not_a_not_b = total_n - a_and_b - b_not_a - a_not_b

    table = [[a_and_b, a_not_b],
            [b_not_a, not_a_not_b]]

    return table

def _fisher_exact(table: list[list[int]]) -> float:
    try:
        pvalue = float(scipy.stats.fisher_exact(table, 'greater')[1])
        return pvalue
    except ValueError:
        return 1.0

def _chi_square(table: list[list[int]]) -> float:
    try:
        pvalue = float(scipy.stats.chi2_contingency(table, 'greater')[1])
        return pvalue
    except ValueError:
        # default to a p-value of 1.0
        # this happens if the sum of a row or column is 0
        return 1.0

def _compute_pvalue(table: list[list[int]], test: str) -> float:
    if test == 'chi-square':
        return _chi_square(table)
    elif test == 'fet':
        return _fisher_exact(table)
    else:
        raise ValueError(f"Unknown statistical test: {test}")

def _compute_sort_ratio(table: list[list[int]]) -> float:
    denom = (table[0][0] + table[1][0])
    if denom == 0:
        return 0

    return table[0][0] / denom

def _compute_prediction_score(fet: float, ratio: float) -> float:
    max_score = 323.0
    # multiplier_for_ratio = 25000  # see comment below
    multiplier_for_ratio = 2500  # see comment below
    if fet == 0.0:
        log_fet_p_value = max_score
    else:
        log_fet_p_value = min(max_score, -math.log10(float(fet)))

    if ratio == 1:
        log_ratio = max_score  # Same max as log_fet_p_value.
    else:
        log_ratio = -math.log10(float(1 - ratio)) * multiplier_for_ratio
        if log_ratio > max_score:
            log_ratio = max_score

    prediction_score = log_fet_p_value + log_ratio
    # so, max possible prediction score will be 646, then normalized below to 2.
    if prediction_score == float(-0):  # change to make display look better.
        prediction_score = float(0)
    normalized_pred_score = prediction_score / max_score
    ##################################  START COMMENT ABOUT multiplier_for_ratio    #############################
    # Note that the fet can range from 1 to 1e-323, which means the -log_fet_p_value will range from zero to 323.
    # The ratio can range from 0 to 1, which means the -log(1-ratio) can  range from 0 to infinity.
    # Typically though, for the hits that have a significant FET, the FET will dominate, because the ratio
    # will be somewhat close to 1. So, for instance if the ratio is 0.3, then the -log(1-ratio) = 0.15.
    # In this case, since it is significant, the FET will be > 1e-5, so the -log10(fet) > 5!
    # Thus, without some multiplier for the ratio, the FET dominates the prediction score for significant hits,
    # which are the ones we care about.
    #
    # A multiplier based on the median value for FET and median value for ratio at first seemed to make
    # sense to me. When I look at some larger queries  involving 20080 B-C pairs
    # (see static/20080_BC_resultsForPredictionScoreNormalization.xlsx),
    # I find:
    # median of fet = 1
    # median of ratio = 0
    #  Thus a multiplier that would make FET and ratio have equal weight would be:
    #   -log10(median_of_log_fet)/(-log10(1- median_of_log_ratio) = UNDEFINED.
    #  UGH!  So basically, the vast majority of B-Cs have zero counts. These would all get
    #  a prediction score of zero, so let's not worry about them.
    #
    # Okay, so if we just look at the ones that have some  B-C counts together (922),
    # I find:
    # median of FET = 0.43019  (-log10(0.43019)  = 0.36634
    # median of ratio = 0.00022 (-log10(1- 0.00022) = 0.000096
    #     -log10(median_of_log_fet)/(-log10(median_of_log_ratio) = 0.36634/0.000096 = 3816.
    # Actually,  I don't think this makes sense either, as we are considering a lot of pairs that are NOT significant.
    #
    # Okay, we are only concerned about ones that are probably significant,
    #  so that is the ones with FET < 1e-5, of which there are 106 in this set.
    #  I find for these 106:
    #  median of fet = 2.67e-21 (-log10(2.67e-21)) = 20.57349
    #  median of ratio = 0.00251  (-log10(1- 0.00251)) =  0.00109
    # so,  the multiplier_for_ratio =
    #     -log10(median_of_log_fet)/(-log10(median_of_log_ratio) = 20.57349/0.00109 =  18874.76.
    #
    # Okay, maybe a better way to go would be to get the median of all the ratios of the log10(fet)/log10(1-ratio)
    #  since we are really interested in the ratio of log10(fet)/log10(1-ratio).
    # Here are some of those median of log10(fet)/log10(1-ratio) values from the spreadsheet:
    # For top 106 significant (FET < 1e-5) ones:  24760
    # For top 528  (FET < 1) ones:   11602
    # For top 922 (ones with B-C count > 0): 1573
    # I'm voting to use the one for the top 106, since for the most part, we are interested in the most significant ones.
    #  So, we will set multiplier_for_ratio = 25000
    #  ACTUALLY, a multiplier_for_ratio = 25000 has the undesirable behaviour that it maxes out at a ratio of ~0.03, which,
    #  is too low.  My (Ron's)  intuition is that the ratio should lead to discrimination in the prediction score up to about 0.25-0.3
    #  So,  let's use a multiplier_for_ratio = 2500.
    ################################# END COMMENT ABOUT multiplier_for_ratio  #####################################

    return normalized_pred_score