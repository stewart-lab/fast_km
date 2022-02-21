import math
from rq import get_current_job
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

        return_pmids = False
        if 'return_pmids' in item:
            return_pmids = bool(item['return_pmids'])

        res = km.kinderminer_search(a_term, b_term, li.the_index, censor_year, return_pmids)

        if 'pmid_intersection' in res:
            res['pmid_intersection'] = str(res['pmid_intersection'])

        return_val.append(res)

    return return_val

def skim_work(json: dict):
    return_val = []

    a_terms = json['a_terms']
    b_terms = json['b_terms']
    c_terms = json['c_terms']
    top_n = json['top_n']
    ab_fet_threshold = json['ab_fet_threshold']

    if 'censor_year' in json:
        censor_year = json['censor_year']
    else:
        censor_year = math.inf

    return_pmids = False
    if 'return_pmids' in json:
        return_pmids = bool(json['return_pmids'])

    if type(top_n) is str:
        top_n = int(top_n)
    if type(censor_year) is str:
        censor_year = int(censor_year)

    for a_term in a_terms:
        ab_results = []

        for b_term in b_terms:
            res = km.kinderminer_search(a_term, b_term, li.the_index, censor_year, return_pmids)

            if res['pvalue'] <= ab_fet_threshold:
                ab_results.append(res)

        # sort by prediction score, descending
        ab_results.sort(key=lambda res: 
            km.get_prediction_score(res['pvalue'], res['sort_ratio']), 
            reverse=True)

        ab_results = ab_results[:top_n]

        # take top N per a-b pair and run b-terms against c-terms
        for i, c_term in enumerate(c_terms):
            for ab in ab_results:
                b_term = ab['b_term']
                bc = km.kinderminer_search(b_term, c_term, li.the_index, censor_year, return_pmids)

                abc_result = {
                        'a_term': ab['a_term'],
                        'b_term': ab['b_term'],
                        'c_term': c_term,

                        'ab_pvalue': ab['pvalue'],
                        'ab_sort_ratio': ab['sort_ratio'],
                        'ab_pred_score': km.get_prediction_score(ab['pvalue'], ab['sort_ratio']),
                        
                        'bc_pvalue': bc['pvalue'],
                        'bc_sort_ratio': bc['sort_ratio'],
                        'bc_pred_score': km.get_prediction_score(bc['pvalue'], bc['sort_ratio']),

                        'a_count': ab['len(a_term_set)'],
                        'b_count': ab['len(b_term_set)'],
                        'c_count': bc['len(b_term_set)'],
                        'ab_count': ab['len(a_b_intersect)'],
                        'bc_count': bc['len(a_b_intersect)'],
                        'total_count': bc['n_articles']
                    }

                if return_pmids:
                    ab = dict()
                    for pmid in sorted(ab['pmid_intersection'], key=li.the_index.citation_count[pmid])[:5]:
                        ab[pmid] = li.the_index.citation_count[pmid]
                    #    ab.append((int(i), int(li.citation_count[i])))
                    #ab.sort(key=compare)
                    
                    bc = []
                    for i in bc['pmid_intersection']:
                        bc.append((int(i), int(li.pmid_citation_count[i])))
                    bc.sort(key=compare)

                    abc_result['ab_pmid_intersection'] = str(ab)
                    abc_result['bc_pmid_intersection'] = str(bc)

                return_val.append(abc_result)
                _update_job_status('progress', i + 1)

    return return_val

def triple_miner_work(json: list):
    km_set = []

    for query in json:
        a_term = query['a_term']
        b_term = query['b_term']
        c_term = query['c_term']

        km_query = dict()
        km_query['a_term'] = a_term + '&&' + b_term
        km_query['b_term'] = c_term

        if 'censor_year' in query:
            km_query['censor_year'] = query['censor_year']

        km_set.append(km_query)

    return km_work(km_set)

def _update_job_status(key, value):
    job = get_current_job()

    if job is None:
        print('error: tried to update job status, but could not find job')
    
    job.meta[key] = value
    job.save_meta()