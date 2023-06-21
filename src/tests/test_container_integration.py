import pytest
import requests
import time
import os
from subprocess import check_output
from indexing import km_util as util
import src.knowledge_graph.knowledge_graph as kg
from src.knowledge_graph.knowledge_graph import KnowledgeGraph

flask_port = '5099'
api_url = 'http://localhost:' + flask_port
km_append = '/km/api/jobs'
skim_append = '/skim/api/jobs'
clear_cache_append = '/clear_cache/api/jobs'
update_index_append = '/update_index/api/jobs'
the_auth = ('username', 'password')

@pytest.fixture
def data_dir():
    return os.path.join(os.getcwd(), "src", "tests", "test_data", "indexer")

@pytest.mark.ci
def test_container_integration_on_running_containers_ci(data_dir):
    # this unit test is meant to be run on a continuous integration platform
    # (e.g., AppVeyor). you can run this on your local machine but you must 
    # have the containers already running with the test index folder as the 
    # PUBMED_DIR env var to run this unit test properly.

    _clear_cache()

    # fail the test if the index has already been built
    index_dir = util.get_index_dir(data_dir)
    assert not os.path.exists(index_dir)

    # populate/query the knowledge graph
    # util.neo4j_host = 'localhost'
    # kg.rel_pvalue_cutoff = 1.1
    # test_kg = KnowledgeGraph()
    # test_kg.populate(os.path.join(data_dir, 'relations.tsv'))
    # test_kg.write_node_id_index(util.get_knowledge_graph_node_id_index(data_dir))
    # test_kg.load_node_id_index(util.get_knowledge_graph_node_id_index(data_dir))
    # query_result = test_kg.query('1 10 phenanthroline', 'phen')
    # assert query_result[0]['relationship'] == 'COREF'
    # assert 18461203 in query_result[0]['pmids']

    # run query WITHOUT the index being built
    skim_url = api_url + skim_append
    query = {
            'a_terms': ['cancer'], 
            'b_terms': ['test'], 
            'c_terms': ['coffee'], 
            'top_n': 50, 
            'ab_fet_threshold': 0.8, 
            'return_pmids': True, 
            'top_n_articles': 1000,
            'query_knowledge_graph': True,
            'rel_pvalue_cutoff': 1.1
        }

    job_info = _post_job(skim_url, query)

    if job_info['status'] == 'failed':
        if 'message' in job_info:
            raise RuntimeError('the job failed because: ' + job_info['message'])
        raise RuntimeError('the job failed without an annotated reason')
    
    result = job_info['result']
    assert not result

    # build the index (but don't download any new files)
    _post_job(api_url + update_index_append, {'n_files': 0, 'clear_cache': False})

    # run query. the new (built) index should be detected, causing the 
    # cache to auto-clear.
    result = _post_job(skim_url, query)['result']
    assert len(result) == 1
    assert result[0]['a_term'] == 'cancer'
    assert result[0]['b_term'] == 'test'
    assert result[0]['ab_pvalue'] == pytest.approx(0.753, abs=0.001)
    assert result[0]['ab_sort_ratio'] == pytest.approx(0.064, abs=0.001)
    assert result[0]['ab_pred_score'] == pytest.approx(0.222, abs=0.001)
    assert result[0]['a_count'] == 303
    assert result[0]['b_count'] == 250
    assert result[0]['ab_count'] == 16
    assert result[0]['total_count'] == 4139
    assert result[0]['c_term'] == 'coffee'
    assert result[0]['bc_pvalue'] == pytest.approx(0.118, abs=0.001)
    assert result[0]['bc_sort_ratio'] == pytest.approx(0.2, abs=0.001)
    assert result[0]['bc_pred_score'] == pytest.approx(0.752, abs=0.001)
    assert result[0]['c_count'] == 10
    assert result[0]['bc_count'] == 2
    assert result[0]['ab_pmid_intersection'] == [34579523, 34580803, 34581509, 34581316, 34579753, 34579789, 34580591, 34580336, 34580018, 34579701, 34579733, 34579095, 34581788, 34579798, 34580026, 34581628]
    assert result[0]['bc_pmid_intersection'] == [34580748, 34578919]
    # assert result[0]['ab_relationship'][0]['relationship'] == 'POS_ASSOCIATION'
    # assert result[0]['ab_relationship'][0]['pmids'] == [1, 2, 3, 4, 5]
    # assert not result[0]['bc_relationship'][0]['relationship']

def _post_job(url, json):
    total_sleep_time = 0
    job_id = requests.post(url=url, json=json, auth=the_auth).json()['id']

    get_response = requests.get(url + '?id=' + job_id, auth=the_auth).json()
    job_status = get_response['status']

    while job_status == 'queued' or job_status == 'started':
        time.sleep(1)
        total_sleep_time += 1
        get_response = requests.get(url + '?id=' + job_id, auth=the_auth).json()
        job_status = get_response['status']

        if total_sleep_time > 300:
            raise RuntimeError('the job timed out after 5 min')

    return get_response

def _clear_cache():
    url = api_url + clear_cache_append
    job_id = requests.post(url=url, json={}, auth=the_auth).json()['id']
    time.sleep(5)