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
        return_val.append(res)

    return return_val

def skim_work(json: dict):
    return_val = []

    a_terms = json['a_terms']
    b_terms = json['b_terms']
    c_terms = json['c_terms']
    top_n = json['top_n']
    censor_year = json['censor_year']

    if type(top_n) is str:
        top_n = int(top_n)
    if type(censor_year) is str:
        censor_year = int(censor_year)

    ab_results = []
    for a_term in a_terms:
        for b_term in b_terms:
            res = km.kinderminer_search(a_term, b_term, li.the_index, censor_year)
            ab_results.append(res)

    # sort by prediction score, descending
    ab_results.sort(key=lambda res: 
        km.get_prediction_score(res['pvalue'], res['sort_ratio']), 
        reverse=True)

    # take top N per a-b pair and run b-terms against c-terms
    for ab in ab_results[:top_n]:
        b_term = ab['b_term']

        for c_term in c_terms:
            bc = km.kinderminer_search(b_term, c_term, li.the_index, censor_year)

            return_val.append(
                {
                    'a_term': ab['a_term'],
                    'b_term': ab['b_term'],
                    'c_term': c_term,
                    'bc_p-value': bc['pvalue'],
                    'ab_pred_score': km.get_prediction_score(ab['pvalue'], ab['sort_ratio'])
                })

    return return_val

def triple_miner_work(json):
    pass