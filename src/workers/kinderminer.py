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
    return table[0][0] / (table[0][0] + table[1][0])

def kinderminer_search(a_term: str, b_term: str, idx: Index, censor_year = math.inf):
    """"""
    
    start_time = time.perf_counter()

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

    # perform fisher's exact test
    pvalue = fisher_exact(table)
    sort_ratio = get_sort_ratio(table)

    run_time = time.perf_counter() - start_time

    return a_term, b_term, len(a_term_set), len(b_term_set), pvalue, sort_ratio, run_time