import pytest
import requests
import time
import os
import shutil
from subprocess import check_output
from indexing import km_util as util

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

def test_container_integration(data_dir, monkeypatch):
    # you can uncomment this pytest.skip if you want to test container 
    # integration on your local machine
    pytest.skip('skipped; CI pipeline tests containers')

    # set the pubmed dir for this test
    monkeypatch.setenv(name='PUBMED_DIR', value=data_dir.replace(os.getcwd(), '.'))

    # use "docker compose" by default, but might need to use "docker-compose" (old syntax)
    # depending on the machine this is being run on
    docker_compose = 'docker compose'

    try:
        cmd_output = check_output(docker_compose, shell=True)
    except:
        #try:
        docker_compose = 'docker-compose'
        #    cmd_output = check_output(docker_compose, shell=True)
        #except:
        #    pytest.skip('skipped; docker compose may not be available on this system')

    try:
        # remove any old containers
        cmd_output = check_output(docker_compose + ' down', shell=True)

        # delete the index if it exists already
        index_dir = util.get_index_dir(data_dir)
        if os.path.exists(index_dir):
            shutil.rmtree(index_dir)
        assert not os.path.exists(index_dir)

        # run the docker containers
        time.sleep(1)
        
        if docker_compose == 'docker compose':
            cmd_output = check_output(docker_compose + ' up --build --wait', shell=True)
            time.sleep(15)
        else:
            # docker-compose does not have the '--wait' flag
            cmd_output = check_output(docker_compose + ' up --build -d', shell=True)
            time.sleep(25)

        # run query
        skim_url = api_url + skim_append
        query = {'a_terms': ['cancer'], 'b_terms': ['coffee'], 'c_terms': ['water'], 'ab_fet_threshold': 1, 'top_n': 50, 'query_knowledge_graph': 'True'}
        job_info = _post_job(skim_url, query)

        if job_info['status'] == 'failed':
            if 'message' in job_info:
                raise RuntimeError('the job failed because: ' + job_info['message'])
            raise RuntimeError('the job failed without an annotated reason')
        
        result = job_info['result']
        assert result[0]['total_count'] == 0

        # TODO: this just tests that the neo4j database can be connected to.
        # it does not test for actual querying of the knowledge graph. need to
        # write that into a test.
        #assert 'ab_relationship' in result[0]
        #assert 'connection error' not in result[0]['ab_relationship']

        # build the index
        _post_job(api_url + update_index_append, {'n_files': 0, 'clear_cache': False})

        # run query. the new (built) index should be detected, causing the 
        # cache to auto-clear.
        result = _post_job(skim_url, query)['result']
        assert result[0]['total_count'] > 4000
        #assert 'ab_relationship' in result[0]
        #assert 'connection error' not in result[0]['ab_relationship']

    except Exception as e:
        assert False, str(e)

    finally:
        cmd_output = check_output(docker_compose + ' down', shell=True)

@pytest.mark.ci
def test_container_integration_on_running_containers_ci(data_dir):
    # you can uncomment this pytest.skip if you want to test container 
    # integration on your local machine. you must have the containers
    # already running with the test index folder as the PUBMED_DIR env var
    # to run this unit test properly.
    #pytest.skip('skipped; CI pipeline tests containers')

    # fail the test if the index has already been built
    index_dir = util.get_index_dir(data_dir)
    assert not os.path.exists(index_dir)

    # run query WITHOUT the index being built
    skim_url = api_url + skim_append
    query = {'a_terms': ['cancer'], 'b_terms': ['test'], 'c_terms': ['coffee'], 'top_n': 50, 'ab_fet_threshold': 0.8}
    job_info = _post_job(skim_url, query)

    if job_info['status'] == 'failed':
        if 'message' in job_info:
            raise RuntimeError('the job failed because: ' + job_info['message'])
        raise RuntimeError('the job failed without an annotated reason')
    
    result = job_info['result']
    assert result[0]['total_count'] == 0

    # build the index (but don't download any new files)
    _post_job(api_url + update_index_append, {'n_files': 0, 'clear_cache': False})

    # run query. the new (built) index should be detected, causing the 
    # cache to auto-clear.
    result = _post_job(skim_url, query)['result']
    assert len(result) == 1
    assert result[0]['a_term'] == 'cancer'
    assert result[0]['b_term'] == 'test'
    assert result[0]['ab_pvalue'] == pytest.approx(0.744, abs=0.001)
    assert result[0]['ab_sort_ratio'] == pytest.approx(0.064, abs=0.001)
    assert result[0]['ab_pred_score'] == pytest.approx(0.222, abs=0.001)
    assert result[0]['a_count'] == 301
    assert result[0]['b_count'] == 250
    assert result[0]['ab_count'] == 16
    assert result[0]['total_count'] == 4139
    assert result[0]['c_term'] == 'coffee'
    assert result[0]['bc_pvalue'] == pytest.approx(0.118, abs=0.001)
    assert result[0]['bc_sort_ratio'] == pytest.approx(0.2, abs=0.001)
    assert result[0]['bc_pred_score'] == pytest.approx(0.752, abs=0.001)
    assert result[0]['c_count'] == 10
    assert result[0]['bc_count'] == 2

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