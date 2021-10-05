import scipy.stats
import time
import math
from .index import Index

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

    # perform fisher's exact test
    table = idx.populate_fet_table(a_term_set, b_term_set, censor_year)

    pvalue = scipy.stats.fisher_exact(table)[1]
    sort_ratio = table[0][0] / (table[0][0] + table[1][0])

    run_time = time.perf_counter() - start_time

    return a_term, b_term, len(a_term_set), len(b_term_set), pvalue, sort_ratio, run_time