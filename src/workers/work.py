import math
import sys
import indexing.index
from rq import get_current_job, Queue
from rq.worker import Worker
from redis import Redis
import rq.command as rqc
import workers.loaded_index as li
import workers.kinderminer as km
from indexing.index_builder import IndexBuilder
import indexing.download_abstracts as downloader

_r = Redis(host='redis', port=6379)
_q = Queue(connection=_r)

def km_work(json: list):
    _initialize_mongo_caching()

    return_val = []

    if len(json) > 1000000000:
        raise ValueError('Must be <=1000000000 queries')

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

def km_work_all_vs_all(json: dict):
    _initialize_mongo_caching()

    return_val = []
    km_only = False

    a_terms = json['a_terms']
    b_terms = json['b_terms']

    if 'c_terms' in json:
        # SKiM query
        c_terms = json['c_terms']

        top_n = json['top_n']
        ab_fet_threshold = json['ab_fet_threshold']
    else:
        # KM query
        km_only = True
        c_terms = ['__KM_ONLY__'] # dummy variable

        if 'top_n' in json:
            top_n = json['top_n']
        else:
            top_n = sys.maxsize

        if 'ab_fet_threshold' in json:
            ab_fet_threshold = json['ab_fet_threshold']
        else:
            ab_fet_threshold = math.inf

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

    for a_term_n, a_term in enumerate(a_terms):
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
        for c_term_n, c_term in enumerate(c_terms):
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
                    abc_result['ab_pmid_intersection'] = str(ab['pmid_intersection'])

                # add c-terms and b-c term KM info (SKiM)
                if not km_only:
                    b_term = ab['b_term']
                    bc = km.kinderminer_search(b_term, c_term, li.the_index, censor_year, return_pmids)

                    abc_result['c_term'] = c_term
                    abc_result['bc_pvalue'] = bc['pvalue']
                    abc_result['bc_sort_ratio'] = bc['sort_ratio']
                    abc_result['bc_pred_score'] = km.get_prediction_score(bc['pvalue'], bc['sort_ratio'])
                    abc_result['c_count'] = bc['len(b_term_set)']
                    abc_result['bc_count'] = bc['len(a_b_intersect)']
                    
                    if return_pmids:
                        abc_result['bc_pmid_intersection'] = str(bc['pmid_intersection'])

                    # report number of C-terms complete
                    _update_job_status('progress', c_term_n + 1)
                else:
                    # report number of A-terms complete
                    _update_job_status('progress', a_term_n + 1)

                return_val.append(abc_result)

    return return_val

def triple_miner_work(json: list):
    _initialize_mongo_caching()
        
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
    print('Checking for files to download...')
    _update_job_status('progress', 'downloading abstracts')

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
    _update_job_status('progress', 'building index')
    index_builder = IndexBuilder(li.pubmed_path)
    index_builder.build_index(overwrite_old=False) # wait to remove old index

    # restart the workers (TODO: except this one)
    _update_job_status('progress', 'restarting workers')
    interrupted_jobs = _restart_workers(requeue_interrupted_jobs=False)

    # remove the old index
    index_builder.overwrite_old_index()

    if clear_cache:
        clear_mongo_cache([])

    # re-queue interrupted jobs
    _queue_jobs(interrupted_jobs)

    _update_job_status('progress', 'finished')

def clear_mongo_cache(json):
    indexing.index._connect_to_mongo()
    indexing.index._empty_mongo()

def _initialize_mongo_caching():
    indexing.index._connect_to_mongo()
    if li.the_index._check_if_mongo_should_be_refreshed():
        clear_mongo_cache([])

        # this second call looks weird, but it's to cache the terms_to_check
        # such as 'fever' to save the current state of the index
        li.the_index._check_if_mongo_should_be_refreshed()

def _restart_workers(requeue_interrupted_jobs = True):
    print('restarting workers...')
    workers = Worker.all(_r)

    interrupted_jobs = []
    this_job = get_current_job()

    for worker in workers:
        # stop any currently-running job
        job = worker.get_current_job()

        if job and str(job.id) != str(this_job.id):
            print('canceling job: ' + str(job.id))
            interrupted_jobs.append(job)

        # TODO: if the worker grabs another job now, it's a problem
        # TODO: prevent >1 concurrent index jobs?

        # shut down the worker
        rqc.send_shutdown_command(_r, worker.name)

    if requeue_interrupted_jobs:
        _queue_jobs(interrupted_jobs)

    return interrupted_jobs

def _queue_jobs(jobs):
    for job in jobs:
        print('restarting job: ' + str(job))
        _q.enqueue_job(job)

def _update_job_status(key, value):
    job = get_current_job()

    if job is None:
        print('error: tried to update job status, but could not find job')
        return
    
    job.meta[key] = value
    job.save_meta()