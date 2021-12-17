import scipy.stats
import time
import math
from indexing.index import Index

logical_or = '||'
logical_and = '&&'
sided = 'greater'

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

    # query the index (handling synonyms if appropriate)
    a_term_set = _construct_abstract_set(a_term, idx)
    b_term_set = _construct_abstract_set(b_term, idx)

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

def _construct_abstract_set(term: str, idx: Index) -> set:
    # TODO: support parenthesis for allowing OR and AND at the same time?
    # e.g., "(cancer||carcinoma) && BRCA1"
    if logical_or in term:
        terms = term.split(logical_or)
        pmid_set = set()
        for synonym in terms:
            pmid_set = pmid_set.update(idx.query_index(synonym))
    elif logical_and in term:
        terms = term.split(logical_and)
        starting_set = idx.query_index(terms[0])
        for t in terms[1:]:
            starting_set.intersection_update(idx.query_index(t))
    else:
        pmid_set = idx.query_index(term)

    return pmid_set