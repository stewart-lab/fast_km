import math
import workers.loaded_index as li
import workers.kinderminer as km

def km_work(json: list):
    return_val = []

    if len(json) > 100000:
        raise ValueError('Must be <=100000 queries')

    for item in json:
        a_term = item['a_term']
        b_term = item['b_term']

        if 'censor_year' in item:
            censor_year = int(item['censor_year'])
        else:
            censor_year = math.inf

        if censor_year is None or censor_year > 2100:
            censor_year = math.inf
        if censor_year < 0:
            censor_year = 0

        if a_term is None or b_term is None:
            raise TypeError('Must supply a_term and b_term')

        res = km.kinderminer_search(a_term, b_term, li.the_index, censor_year)
        res_aterm = res[0]
        res_bterm = res[1]
        res_len_aterm_set = res[2]
        res_len_bterm_set = res[3]
        res_pvalue = res[4]
        res_sort_ratio = res[5]
        res_time = res[6]
        res_a_and_b = res[7]
        res_n_articles = res[8]

        return_val.append(
            {
                'a_term' : res_aterm,
                'b_term' : res_bterm,
                'len_a_term_set' : str(res_len_aterm_set),
                'len_b_term_set' : str(res_len_bterm_set),
                'p-value': str(res_pvalue),
                'sort_ratio': str(res_sort_ratio),
                'query_time': str(res_time),
                'n_a_and_b': str(res_a_and_b),
                'n_articles': str(res_n_articles)
            })

    return return_val

def skim_work(json: dict):
    return_val = []

    a_terms = json['a_terms']
    b_terms = json['b_terms']
    c_terms = json['c_terms']
    top_n = 50

    significant_ab_queries = []
    for a_term in a_terms:
        for b_term in b_terms:
            res = km.kinderminer_search(a_term, b_term, li.the_index, math.inf)
            pvalue = res[4]
            sort_ratio = res[5]

            if pvalue < 1e10-5:
                pred_score = -math.log10(max(pvalue, 10e-30)) + math.log10(sort_ratio)
                significant_ab_queries.append((a_term, b_term, pred_score))

    # sort by prediction score, descending
    significant_ab_queries.sort(key=lambda query: query[2], reverse=True)

    for ab in significant_ab_queries[:top_n]:
        b_term = ab[1]
        for c_term in c_terms:
            res = km.kinderminer_search(b_term, c_term, li.the_index, math.inf)
            return_val.append(
                {
                    'a_term': ab[0],
                    'b_term': ab[1],
                    'c_term': c_term,
                    'bc_p-value': res[4],
                    'ab_pred_score': ab[2]
                })

    return return_val

def triple_miner_work(json):
    pass