import math
import workers.loaded_index as li
import workers.kinderminer as km

def km_work(json):
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