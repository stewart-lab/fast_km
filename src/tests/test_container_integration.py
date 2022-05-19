import pytest
import requests
import time
import os
import shutil
from subprocess import check_call, check_output
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

def test_api(data_dir):
    # TODO: this is very hacky. it replaces the .env file with one just
    # for this unit test. --env-file docker param doesn't seem to work.
    # ideally would find some alternative to this...
    env_file = os.path.join(os.getcwd(), '.env')
    shell_file = os.path.join(os.getcwd(), 'container_integration_test.sh')
    tmp_env_file = os.path.join(os.getcwd(), 'tmp')
    os.rename(env_file, tmp_env_file)

    with open(env_file, 'w') as f:
        f.write('PUBMED_DIR=\"' + data_dir + "\"")
        f.write('\n')
        f.write('PASSWORD_HASH="____2b____12____YfgpDEOwxLy..UkZEe0H8.0aO/AQXpbsA4sAgZ9RWQShkG4iZYl16"')
    
    with open(shell_file, 'w') as f:
        f.write('docker compose up --build --wait --force-recreate -V')

    try:
        cmd_output = check_output("docker compose down", shell=True)
    except:
        pytest.skip('skipped; docker compose is not available on this system')

    try:
        # delete the index if it exists already
        index_dir = util.get_index_dir(data_dir)
        if os.path.exists(index_dir):
            shutil.rmtree(index_dir)
        assert not os.path.exists(index_dir)

        # run the docker containers
        time.sleep(1)
        cmd_output = check_output("source " + 'container_integration_test.sh', shell=True)
        time.sleep(15)

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
        cmd_output = check_output("docker compose down", shell=True)
        os.rename(tmp_env_file, env_file)

def _post_job(url, json):
    job_id = requests.post(url=url, json=json, auth=the_auth).json()['id']

    get_response = requests.get(url + "?id=" + job_id, auth=the_auth).json()
    job_status = get_response['status']

    while job_status == 'queued' or job_status == 'started':
        time.sleep(1)
        get_response = requests.get(url + "?id=" + job_id, auth=the_auth).json()
        job_status = get_response['status']

    return get_response