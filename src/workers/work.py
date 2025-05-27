import math
import sys
import os
import indexing.index
from rq import get_current_job, Queue
from rq.worker import Worker
from redis import Redis
import rq.command as rqc
import workers.loaded_index as li
import workers.kinderminer as km
import workers.skim_gpt as skim_gpt
from indexing.index_builder import IndexBuilder
import indexing.download_abstracts as downloader
from knowledge_graph.knowledge_graph import KnowledgeGraph, rel_pvalue_cutoff
import indexing.km_util as km_util
import indexing.index as index

redis_host = km_util.redis_address.split(':')[0]
redis_port = int(km_util.redis_address.split(':')[1])
_r = Redis(host=redis_host, port=redis_port)

def km_work_all_vs_all(json: dict):
    _initialize_mongo_caching()
    knowledge_graphs = connect_to_neo4j()

    return_val = []
    km_only = False

    a_terms = json['a_terms']
    b_terms = json['b_terms']
    scoring = json.get('scoring', 'fet')

    if 'c_terms' in json:
        # SKiM query
        c_terms = json['c_terms']

        top_n = _get_top_n(json)
        ab_fet_threshold = _get_ab_fet_threshold(json, 1e-5)
        bc_fet_threshold = _get_bc_fet_threshold(json, 0.9999)
        valid_bc_hit_pval = float(json.get('valid_bc_hit_pval', 1.0))
    else:
        # KM query
        km_only = True
        c_terms = ['__KM_ONLY__'] # dummy variable

        top_n = _get_top_n(json, sys.maxsize)
        ab_fet_threshold = _get_ab_fet_threshold(json, math.inf)

    censor_year = _get_censor_year(json)
    return_pmids = bool(json.get('return_pmids', False))
    query_kg = bool(json.get('query_knowledge_graph', False))
    _rel_pvalue_cutoff = float(json.get('rel_pvalue_cutoff', rel_pvalue_cutoff))

    # 'top_n_articles' is here for legacy support. 'top_n_articles_most_cited' is the new key.
    if ('top_n_articles' in json):
        top_n_articles_most_cited = int(json['top_n_articles'])
        top_n_articles_most_recent = 0
    else:
        top_n_articles_most_cited = int(json.get('top_n_articles_most_cited', 5))
        top_n_articles_most_recent = int(json.get('top_n_articles_most_recent', 5))

    _update_job_status('progress', 0)

    b_term_token_dict = _get_token_dict(b_terms)

    for a_term_n, a_term in enumerate(a_terms):
        ab_results = []
        b_term_set = list(b_terms)

        while a_term in b_term_set:
            b_term_set.remove(a_term)

        b_term_n = 0

        while b_term_set:
            b_term = li.the_index.get_highest_priority_term(b_term_set, b_term_token_dict)

            if b_term in b_term_set:
                b_term_set.remove(b_term)

            ab = km.kinderminer_search(a_term, b_term, li.the_index, censor_year, return_pmids, 
                                       top_n_articles_most_cited, top_n_articles_most_recent, scoring)

            if ab['pvalue'] <= ab_fet_threshold:
                ab_results.append(ab)
            else:
                # RAM efficiency. decache unneeded tokens/terms
                li.the_index.decache_token(b_term)

            _remove_from_token_dict(b_term, b_term_token_dict)

            # report KM progress
            if km_only:
                progress = _km_progress(a_term_n, b_term_n + 1, len(a_terms), len(b_terms))
                _update_job_status('progress', progress)

            b_term_n += 1

        # sort by prediction score, descending
        ab_results.sort(key=lambda res: 
            km.get_prediction_score(res['pvalue'], res['sort_ratio']), 
            reverse=True)

        ab_results = ab_results[:top_n + 20]

        # RAM efficiency. decache unneeded tokens/terms
        b_terms_used = set([ab_res['b_term'] for ab_res in ab_results])
        c_term_token_dict = _get_token_dict(c_terms)

        _items = list(li.the_index._token_cache.keys())
        _items.extend(list(li.the_index._query_cache.keys()))

        for token in _items:
            if token not in b_terms_used:
                li.the_index.decache_token(token)

        # take top N per a-b pair and run b-terms against c-terms
        c_term_set = list(c_terms)

        while a_term in c_term_set:
            c_term_set.remove(a_term)

        c_term_n = 0

        # for each c-term, run each b-term against it
        while c_term_set:
            c_term = li.the_index.get_highest_priority_term(c_term_set, c_term_token_dict)

            if c_term in c_term_set:
                c_term_set.remove(c_term)

            # run A-C
            if not km_only:
                ac = km.kinderminer_search(a_term, c_term, li.the_index, censor_year, return_pmids, 
                    top_n_articles_most_cited, top_n_articles_most_recent, scoring)

            for ab in ab_results:
                abc_result = {
                        'a_term': ab['a_term'],
                        'b_term': ab['b_term'],

                        'ab_pvalue': ab['pvalue'],
                        'ab_sort_ratio': ab['sort_ratio'],
                        'ab_pred_score': km.get_prediction_score(ab['pvalue'], ab['sort_ratio']),
                        
                        'a_count': ab['len(a_term_set)'],
                        'b_count': ab['len(b_term_set)'],
                        'ab_count': ab['len(a_b_intersect)'],
                        'total_count': ab['n_articles']
                    }

                if return_pmids:
                    abc_result['ab_pmid_intersection'] = ab['pmid_intersection']

                if query_kg:
                    if abc_result['ab_pvalue'] < _rel_pvalue_cutoff:
                        abc_result['ab_relationship'] = []

                        for kg in knowledge_graphs:
                            rel = kg.query(abc_result['a_term'], abc_result['b_term'], censor_year)
                            abc_result['ab_relationship'].extend(rel)
                    else:
                        abc_result['ab_relationship'] = None

                # add c-terms and b-c term KM info (SKiM)
                if not km_only:
                    b_term = ab['b_term']

                    if b_term == c_term:
                        continue

                    bc = km.kinderminer_search(b_term, c_term, li.the_index, censor_year, return_pmids, 
                                               top_n_articles_most_cited, top_n_articles_most_recent, scoring)

                    abc_result['c_term'] = c_term
                    abc_result['bc_pvalue'] = bc['pvalue']
                    abc_result['bc_sort_ratio'] = bc['sort_ratio']
                    abc_result['bc_pred_score'] = km.get_prediction_score(bc['pvalue'], bc['sort_ratio'])
                    abc_result['c_count'] = bc['len(b_term_set)']
                    abc_result['bc_count'] = bc['len(a_b_intersect)']

                    abc_result['ac_pvalue'] = ac['pvalue']
                    abc_result['ac_sort_ratio'] = ac['sort_ratio']
                    abc_result['ac_pred_score'] = km.get_prediction_score(ac['pvalue'], ac['sort_ratio'])
                    abc_result['ac_count'] = ac['len(a_b_intersect)']
                    
                    if return_pmids:
                        abc_result['bc_pmid_intersection'] = bc['pmid_intersection']
                        abc_result['ac_pmid_intersection'] = ac['pmid_intersection']

                    if query_kg:
                        if abc_result['bc_pvalue'] < _rel_pvalue_cutoff:
                            abc_result['bc_relationship'] = []

                            for kg in knowledge_graphs:
                                rel = kg.query(abc_result['b_term'], abc_result['c_term'], censor_year)
                                abc_result['bc_relationship'].extend(rel)
                        else:
                            abc_result['bc_relationship'] = None

                if km_only or (abc_result['bc_pvalue'] <= bc_fet_threshold):
                    return_val.append(abc_result)

            if not km_only:
                # report SKiM progress - percentage of C-terms complete
                progress = _skim_progress(a_term_n, b_term_n, c_term_n + 1, len(a_terms), len(b_terms), len(c_terms))
                _update_job_status('progress', progress)

                # RAM efficiency. decache unneeded tokens/terms
                _remove_from_token_dict(c_term, c_term_token_dict)

            c_term_n += 1

    if top_n < sys.maxsize:
        # sometimes high prediction score A-B pairs with no B-C pairs will 
        # crowd out lower-scoring A-B pairs that have B-C pairs. we want to
        # ignore the former in favor of including the latter. we added 
        # 20 extra B-terms above as padding, now we need to filter out any
        # extra B-terms not in the top N.

        # Bs including padding in prediction score order, including those without B-C hits
        ranked_bs = [ab['b_term'] for ab in ab_results]

        # Bs with valid B-C hits
        valid_bs = set([x['b_term'] for x in return_val if x['bc_pvalue'] <= valid_bc_hit_pval])

        # top 50 Bs with valid B-C hits
        ranked_top_n_valid_bs = set([b for b in ranked_bs if b in valid_bs][:top_n])

        # filter the results
        return_val = [x for x in return_val if x['b_term'] in ranked_top_n_valid_bs]

    _update_job_status('progress', 1.0000)
    return return_val

def gpt_score_hypothesis(json: dict):
    data = json.get('data', None)

    if not data or len(data) == 0:
        raise ValueError('data is required and must be a non-empty list')
    
    # determine if KM or SKiM
    count_with_c = len([x for x in data if 'c_term' in x])
    if count_with_c != 0 and count_with_c != len(data):
        raise ValueError('data must be all KM or all SKiM')
    is_km = 'c_term' not in data[0]


    # validate data
    for item in data:
        if 'a_term' not in item:
            raise ValueError('a_term is required')
        if 'b_term' not in item:
            raise ValueError('b_term is required')
        if not is_km and 'c_term' not in item:
            raise ValueError('c_term is required for SKiM')
        
        if 'ab_pmid_intersection' not in item or len(item['ab_pmid_intersection']) == 0:
            raise ValueError('ab_pmid_intersection is required')
        if not is_km and ('bc_pmid_intersection' not in item or len(item['bc_pmid_intersection'])) == 0:
            raise ValueError('bc_pmid_intersection is required for SKiM')
        if not is_km and ('ac_pmid_intersection' not in item or len(item['ac_pmid_intersection'])) == 0:
            raise ValueError('ac_pmid_intersection is required for SKiM')
    
    # validate hypotheses
    if is_km:
        if 'KM_hypothesis' not in json:
            raise ValueError('KM_hypothesis is required for KM')
        if not isinstance(json['KM_hypothesis'], str):
            raise ValueError('KM_hypothesis must be a string')
        if '{a_term}' not in json['KM_hypothesis'] or '{b_term}' not in json['KM_hypothesis']:
            raise ValueError('KM_hypothesis must contain {a_term} and {b_term}')
    if not is_km:
        if 'SKIM_hypotheses' not in json:
            raise ValueError('SKIM_hypotheses is required for SKiM')
        if not isinstance(json['SKIM_hypotheses'], dict):
            raise ValueError('SKIM_hypotheses must be a dictionary')
        if 'AB' not in json['SKIM_hypotheses']:
            raise ValueError('AB hypothesis is required for SKiM')
        if 'BC' not in json['SKIM_hypotheses']:
            raise ValueError('BC hypothesis is required for SKiM')
        if 'rel_AC' not in json['SKIM_hypotheses']:
            raise ValueError('rel_AC hypothesis is required for SKiM')
        if 'AC' not in json['SKIM_hypotheses']:
            raise ValueError('AC hypothesis is required for SKiM')
        if 'ABC' not in json['SKIM_hypotheses']:
            raise ValueError('ABC hypothesis is required for SKiM')
        
        if '{a_term}' not in json['SKIM_hypotheses']['AB'] or '{b_term}' not in json['SKIM_hypotheses']['AB']:
            raise ValueError('AB hypothesis must contain {a_term} and {b_term}')
        if '{b_term}' not in json['SKIM_hypotheses']['BC'] or '{c_term}' not in json['SKIM_hypotheses']['BC']:
            raise ValueError('BC hypothesis must contain {b_term} and {c_term}')
        if '{a_term}' not in json['SKIM_hypotheses']['AC'] or '{c_term}' not in json['SKIM_hypotheses']['AC']:
            raise ValueError('AC hypothesis must contain {a_term} and {c_term}')
        if '{a_term}' not in json['SKIM_hypotheses']['rel_AC'] or '{c_term}' not in json['SKIM_hypotheses']['rel_AC']:
            raise ValueError('rel_AC hypothesis must contain {a_term} and {c_term}')
        if '{a_term}' not in json['SKIM_hypotheses']['ABC'] or '{b_term}' not in json['SKIM_hypotheses']['ABC'] or '{c_term}' not in json['SKIM_hypotheses']['ABC']:
            raise ValueError('ABC hypothesis must contain {a_term}, {b_term}, and {c_term}')
        
    # trim PMID AB-intersects, BC-intersects, and AC-intersects
    top_n_articles_most_cited = int(json.get('top_n_articles_most_cited', 0))
    top_n_articles_most_recent = int(json.get('top_n_articles_most_recent', 0))
    if top_n_articles_most_cited <= 0 and top_n_articles_most_recent <= 0:
        top_n_articles_most_cited = 3
        top_n_articles_most_recent = 2

    for item in data:
        if 'ab_pmid_intersection' in item:
            ab_pmid_intersection = [int(pmid) for pmid in item['ab_pmid_intersection']]
            most_cited = li.the_index.top_n_by_citation_count(ab_pmid_intersection, top_n_articles_most_cited)
            most_recent = li.the_index.top_n_by_pmid(ab_pmid_intersection, top_n_articles_most_recent)
            item['ab_pmid_intersection'] = [str(pmid) for pmid in set(most_cited + most_recent)]
        if 'bc_pmid_intersection' in item:
            bc_pmid_intersection = [int(pmid) for pmid in item['bc_pmid_intersection']]
            most_cited = li.the_index.top_n_by_citation_count(bc_pmid_intersection, top_n_articles_most_cited)
            most_recent = li.the_index.top_n_by_pmid(bc_pmid_intersection, top_n_articles_most_recent)
            item['bc_pmid_intersection'] = [str(pmid) for pmid in set(most_cited + most_recent)]
        if 'ac_pmid_intersection' in item:
            ac_pmid_intersection = [int(pmid) for pmid in item['ac_pmid_intersection']]
            most_cited = li.the_index.top_n_by_citation_count(ac_pmid_intersection, top_n_articles_most_cited)
            most_recent = li.the_index.top_n_by_pmid(ac_pmid_intersection, top_n_articles_most_recent)
            item['ac_pmid_intersection'] = [str(pmid) for pmid in set(most_cited + most_recent)]
    
    job = get_current_job()
    temp_dir = os.path.join('/tmp', "job-" + job.id)
    results = skim_gpt.run_skim_gpt(temp_dir, json)
    return results

def update_index_work(json: dict):
    indexing.index._connect_to_mongo()
    if 'n_files' in json:
        n_files = json['n_files']
    else:
        n_files = math.inf
    if 'clear_cache' in json:
        clear_cache = json['clear_cache']
    else:
        clear_cache = True

    # download baseline
    print('INFO: Checking for files to download...')
    _update_job_status('progress', 0.1000)

    downloader.bulk_download(
        ftp_address='ftp.ncbi.nlm.nih.gov',
        ftp_dir='pubmed/baseline',
        local_dir=li.pubmed_path,
        n_to_download=n_files
    )

    # download daily updates
    downloader.bulk_download(
        ftp_address='ftp.ncbi.nlm.nih.gov',
        ftp_dir='pubmed/updatefiles',
        local_dir=li.pubmed_path,
        n_to_download=n_files
    )

    # TODO: figure out how to report download and index building progress
    _update_job_status('progress', 0.3000)
    index_builder = IndexBuilder(li.pubmed_path)
    index_builder.build_index(overwrite_old=False) # wait to remove old index

    # restart the workers (TODO: except this one)
    _update_job_status('progress', 0.9000)
    interrupted_jobs = restart_workers(requeue_interrupted_jobs=False)

    # remove the old index
    index_builder.overwrite_old_index()

    if clear_cache:
        clear_mongo_cache([])

    # re-queue interrupted jobs
    _queue_jobs(interrupted_jobs)

    _update_job_status('progress', 1.0000)

def clear_mongo_cache(json):
    indexing.index._connect_to_mongo()
    indexing.index._empty_mongo()

def restart_workers(requeue_interrupted_jobs = True):
    print('INFO: restarting workers...')
    workers = Worker.all(_r)

    interrupted_jobs = []
    this_job = get_current_job()

    for worker in workers:
        # stop any currently-running job
        job = worker.get_current_job()

        if job and (not this_job or (str(job.id) != str(this_job.id))):
            print('INFO: canceling job: ' + str(job.id))
            interrupted_jobs.append(job)

        # TODO: if the worker grabs another job now, it's a problem
        # TODO: prevent >1 concurrent index jobs?

        # shut down the worker
        rqc.send_shutdown_command(_r, worker.name)

    if requeue_interrupted_jobs:
        _queue_jobs(interrupted_jobs)

    return interrupted_jobs

def _initialize_mongo_caching():
    indexing.index._connect_to_mongo()
    if li.the_index._check_if_mongo_should_be_refreshed():
        clear_mongo_cache([])

        # this second call looks weird, but it's to cache the terms_to_check
        # such as 'fever' to save the current state of the index
        li.the_index._check_if_mongo_should_be_refreshed()

def connect_to_neo4j() -> 'list[KnowledgeGraph]':
    graphs = []
    for url in km_util.neo4j_addresses:
        graphs.append(KnowledgeGraph(url))
    return graphs

def _queue_jobs(jobs):
    for job in jobs:
        print('INFO: restarting job: ' + str(job))
        if 'priority' in job:
            job_priority = job['priority']
        else:
            job_priority = km_util.JobPriority.MEDIUM.name
        _q = Queue(name=job_priority, connection=_r)
        _q.enqueue_job(job)

def _update_job_status(key, value):
    job = get_current_job()

    if job is None:
        print('WARNING: tried to update job status, but could not find job')
        return
    
    job.meta[key] = value
    job.save_meta()

def _get_censor_year(item):
    if 'censor_year' in item:
        censor_year = int(item['censor_year'])
    else:
        censor_year = math.inf

    if censor_year is None or censor_year > 2100:
        censor_year = math.inf
    if censor_year < 0:
        censor_year = 0

    return censor_year

def _get_top_n(the_dict: dict, default_val = 50):
    return int(the_dict.get('top_n', default_val))

def _get_ab_fet_threshold(the_dict: dict, default_val = 1e-5):
    return float(the_dict.get('ab_fet_threshold', default_val))

def _get_bc_fet_threshold(the_dict: dict, default_val = 0.9999):
    return float(the_dict.get('bc_fet_threshold', default_val))

def _km_progress(a_complete: int, b_complete: int, a_total: int, b_total: int):
    numerator = a_complete * b_total + b_complete
    denom = a_total * b_total
    progress = round(numerator / denom, 4)
    return min(progress, 0.9999)

def _skim_progress(a_complete: int, b_complete: int, c_complete: int, a_total: int, b_total: int, c_total: int):
    # numerator = (a_complete * b_complete) + c_complete + 1
    # denom = a_total * b_total * c_total
    progress = round(c_complete / c_total, 4)
    return min(progress, 0.9999)

def _get_token_dict(c_terms: 'list[str]'):
    c_term_token_dict = dict()
    for c_term in c_terms:
        subterms = index.get_subterms(c_term)

        for subterm in subterms:
            # add the tokens
            c_tokens = km_util.get_tokens(subterm)
            c_tokens = li.the_index.get_ngrams(c_tokens)
            for c_token in c_tokens:
                if c_token not in c_term_token_dict:
                    c_term_token_dict[c_token] = []
                c_term_token_dict[c_token].append(c_term)

            # add the subterm
            if subterm not in c_term_token_dict:
                c_term_token_dict[subterm] = []
            c_term_token_dict[subterm].append(c_term)
    
    return c_term_token_dict

def _remove_from_token_dict(term: str, token_dict):
    li.the_index.decache_token(term)
    subterms = index.get_subterms(term)

    for subterm in subterms:
        c_tokens = km_util.get_tokens(subterm)
        c_tokens = li.the_index.get_ngrams(c_tokens)
        for c_token in c_tokens:
            query_terms = token_dict[c_token]
            if term in query_terms:
                query_terms.remove(term)
            if not query_terms:
                li.the_index.decache_token(c_token)

        query_terms = token_dict[subterm]
        if term in query_terms:
            query_terms.remove(term)
        if not query_terms:
            li.the_index.decache_token(subterm)
