import scipy.stats
import time
import math
from indexing.index import Index

sided = 'greater'

def get_contingency_table(a_term_set: set, b_term_set: set, total_n: int):
    """Populates the table for the Fisher's exact test"""
    a_and_b = len(a_term_set & b_term_set)
    not_a_b = len(b_term_set) - a_and_b
    a_not_b = len(a_term_set) - a_and_b
    not_a_term_not_b_term = total_n - a_and_b - not_a_b - a_not_b

    table = [[a_and_b, a_not_b],
            [not_a_b, not_a_term_not_b_term]]

    return table

def fisher_exact(table) -> float:
    return scipy.stats.fisher_exact(table, sided)[1]

def get_sort_ratio(table) -> float:
    denom = (table[0][0] + table[1][0])
    if denom == 0:
        return 0 # TODO?

    return table[0][0] / denom

def kinderminer_search(a_term: str, b_term: str, idx: Index, censor_year = math.inf) -> dict:
    """"""
    start_time = time.perf_counter()
    result = dict()

    # query the index
    a_term_set = idx.query_index(a_term)
    b_term_set = idx.query_index(b_term)

    # censor by year if applicable
    if censor_year is not math.inf:
        a_term_set = idx.censor_by_year(a_term_set, censor_year)
        b_term_set = idx.censor_by_year(b_term_set, censor_year)

    # create contingency table
    table = get_contingency_table(a_term_set, b_term_set, 
        idx.n_articles(censor_year))

    n_a_and_b = table[0][0]
    n_articles = idx.n_articles(censor_year)

    # perform fisher's exact test
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

    return result

def get_prediction_score(pvalue: float, sort_ratio: float):
    max_score = 323.0
    multiplier_for_ratio = 2500

    # calculate log p-value
    if pvalue == 0.0:
        log_pvalue = max_score
    else:
        log_pvalue = -math.log10(pvalue)

    # calculate log sort ratio
    if sort_ratio == 1:
        log_sort_ratio = max_score
    else:
        log_sort_ratio = -math.log10(float(1 - sort_ratio)) * multiplier_for_ratio
        log_sort_ratio = min(max_score, log_sort_ratio)

    # prediction score = log pvalue + log sort ratio
    return log_pvalue + log_sort_ratio