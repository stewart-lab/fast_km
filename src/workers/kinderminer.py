import scipy.stats
import time
import math
from indexing.index import Index

fet_sided = 'greater'

def get_contingency_table(a_term_set: set, b_term_set: set, total_n: int):
    """Populates the table for the Fisher's exact test"""
    a_and_b = len(a_term_set & b_term_set)
    b_not_a = len(b_term_set) - a_and_b
    a_not_b = len(a_term_set) - a_and_b
    not_a_not_b = total_n - a_and_b - b_not_a - a_not_b

    table = [[a_and_b, a_not_b],
            [b_not_a, not_a_not_b]]

    return table

def fisher_exact(table) -> float:
    return scipy.stats.fisher_exact(table, fet_sided)[1]

def chi_square(table) -> float:
    try:
        return scipy.stats.chi2_contingency(table, fet_sided)[1]
    except ValueError:
        # default to a p-value of 1.0
        # this happens if the sum of a row or column is 0
        return 1.0

def get_sort_ratio(table) -> float:
    denom = (table[0][0] + table[1][0])
    if denom == 0:
        return 0 # TODO?

    return table[0][0] / denom

def kinderminer_search(a_term: str, b_term: str, idx: Index, censor_year = math.inf, 
                       return_pmids = False, top_n_articles_most_cited = math.inf,
                       top_n_articles_most_recent = math.inf, scoring = 'fet') -> dict:
    """"""
    start_time = time.perf_counter()
    result = dict()

    # query the index (handling synonyms if appropriate)
    a_term_set = idx.construct_abstract_set(a_term)
    b_term_set = idx.construct_abstract_set(b_term)

    # censor by year if applicable
    if censor_year is not math.inf:
        a_term_set = idx.censor_by_year(a_term_set, censor_year, a_term)
        b_term_set = idx.censor_by_year(b_term_set, censor_year, b_term)

    # create contingency table
    table = get_contingency_table(a_term_set, b_term_set, 
        idx.n_articles(censor_year))

    n_a_and_b = table[0][0]
    n_articles = idx.n_articles(censor_year)

    # perform statistical test (default fisher's exact test)
    if scoring == 'chi-square':
        pvalue = chi_square(table)
    else: # 'fet'
        pvalue = fisher_exact(table)

    sort_ratio = get_sort_ratio(table)

    run_time = time.perf_counter() - start_time

    result['a_term'] = a_term
    result['b_term'] = b_term
    result['len(a_term_set)'] = len(a_term_set)
    result['len(b_term_set)'] = len(b_term_set)
    result['pvalue'] = pvalue
    result['sort_ratio'] = sort_ratio
    result['run_time'] = run_time
    result['len(a_b_intersect)'] = n_a_and_b
    result['n_articles'] = n_articles

    if return_pmids:
        ab_intersect = a_term_set & b_term_set

        if (top_n_articles_most_cited is not math.inf) and (top_n_articles_most_recent is not math.inf):
            n_most_cited = idx.top_n_by_citation_count(ab_intersect, top_n_articles_most_cited)
            n_most_recent = idx.top_n_by_pmid(ab_intersect, top_n_articles_most_recent)

            for pmid in n_most_recent:
                if pmid not in n_most_cited:
                    n_most_cited.append(pmid)
            append_pmids = n_most_cited
        else:
            append_pmids = idx.top_n_by_citation_count(ab_intersect, math.inf)

        result['pmid_intersection'] = append_pmids

    return result

def get_prediction_score(fet, ratio):
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