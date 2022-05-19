import pytest
import requests
import time
import os
import shutil
from subprocess import check_output
from indexing import km_util as util
# the tests in this file test the Docker container communication, job
# queuing, mongoDB caching, etc. They require that the API server and workers
# be running with supporting containers in a docker-compose environment.

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

def test_container_integration(data_dir, monkeypatch, capfd):
    # set the pubmed dir for this test
    monkeypatch.setenv(name='PUBMED_DIR', value='\"' + data_dir + '\"')

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
        query = {'a_terms': ['cancer'], 'b_terms': ['coffee'], 'c_terms': ['water'], 'ab_fet_threshold': 1, 'top_n': 50}
        result = _post_job(skim_url, query)['result']
        assert result[0]['total_count'] == 0

        # build the index
        _post_job(api_url + update_index_append, {'n_files': 0, 'clear_cache': False})

        # run query. the new (built) index should be detected, causing the 
        # cache to auto-clear.
        result = _post_job(skim_url, query)['result']
        assert result[0]['total_count'] > 4000

    except Exception as e:
        assert False, str(e)

    finally:
        cmd_output = check_output(docker_compose + ' down', shell=True)

def _post_job(url, json):
    job_id = requests.post(url=url, json=json, auth=the_auth).json()['id']

    get_response = requests.get(url + '?id=' + job_id, auth=the_auth).json()
    job_status = get_response['status']

    while job_status == 'queued' or job_status == 'started':
        time.sleep(1)
        get_response = requests.get(url + '?id=' + job_id, auth=the_auth).json()
        job_status = get_response['status']

    return get_response