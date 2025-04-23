import os
import shutil
import json
from glob import glob
from condor.htcondor_helper import HTCondorHelper
import indexing.km_util as km_util

def run_skim_gpt(job_dir: str, config: dict) -> 'list[dict]':
    """
    Submit a SKiM-GPT or KM-GPT job to HTCondor and return the results.
    """

    data = config['data']
    is_km = 'c_term' not in data[0]

    # fill in remainder of config
    if is_km:
        config['JOB_TYPE'] = 'km_with_gpt'
        config['SKIM_hypotheses'] = dict()
    else:
        config['JOB_TYPE'] = 'skim_with_gpt'
        config['KM_hypothesis'] = ''

    config['Evaluate_single_abstract'] = False

    config['GLOBAL_SETTINGS'] = {
        "A_TERM": "",
        "A_TERM_SUFFIX": "",
        "TOP_N_ARTICLES_MOST_CITED": 50,
        "TOP_N_ARTICLES_MOST_RECENT": 50,
        "POST_N": 5,
        "MIN_WORD_COUNT": 98,
        "API_URL": "http://localhost:5099/skim/api/jobs",
        "PORT": "5081",
        "RATE_LIMIT": 3,
        "DELAY": 10,
        "MAX_RETRIES": 10,
        "RETRY_DELAY": 5,
        "LOG_LEVEL": "INFO"
    }

    config['abstract_filter'] = {
        "MODEL": "lexu14/porpoise1",
        "TEMPERATURE": 0,
        "TOP_K": 20,
        "TOP_P": 0.95,
        "MAX_COT_TOKENS": 500,
        "DEBUG": False,
        "TEST_LEAKAGE": False,
        "TEST_LEAKAGE_TYPE": "empty"
    }

    # default to o3-mini model
    # TODO: make the default model an environment variable
    config['GLOBAL_SETTINGS']['MODEL'] = config.get('model', 'o3-mini')

    config['PUBMED_API_KEY'] = km_util.pubmed_api_key
    config['API_KEY'] = km_util.openai_api_key
    config['HTCONDOR_TOKEN'] = km_util.htcondor_token

    if not config['PUBMED_API_KEY']:
        raise ValueError("PUBMED_API_KEY is not set")
    if not config['API_KEY']:
        raise ValueError("OPENAI_API_KEY is not set")

    # TODO: make these environment variables
    CHTC_COLLECTOR_HOST="cm.chtc.wisc.edu"
    CHTC_SUBMIT_HOST="ap2002.chtc.wisc.edu"

    htcondor_connection_config = {
        "collector_host": CHTC_COLLECTOR_HOST,
        "submit_host": CHTC_SUBMIT_HOST,
        "token": km_util.htcondor_token,
    }

    if not htcondor_connection_config['token']:
        raise ValueError("HTCONDOR_TOKEN is not set")

    htcondor_job_config = {
        # docker image details
        'universe': 'docker',
        "docker_image": "docker://lexu27/kmgpt_filter:v0.6",
        "docker_pull_policy": "missing",
        
        # this transfers files from the submit node to the execute node
        # the 'spooling' stuff below transfers the files from the current machine to the submit node
        'should_transfer_files': 'YES',
        'transfer_input_files': '.', 

        # this command is run on the execute node
        'executable': 'run.sh',

        # execute node logging
        'log': 'run_$(Cluster).log',
        'error': 'run_$(Cluster)_$(Process).err',
        'output': 'run_$(Cluster)_$(Process).out',
        "stream_error": "true",
        "stream_output": "true",

        # this transfers files from the execute node to the submit node
        # the 'retrieve_output' in htcondor_helper.py transfers the files from the submit node to the current machine
        "transfer_output_files": "job_result.json",
        "when_to_transfer_output": "ON_EXIT",

        # hardware/CUDA requirements for the execute node
        "request_gpus": "1",
        "request_cpus": "1",
        "request_memory": "24GB",
        "request_disk": "60GB",
        "gpus_minimum_memory": "30G",
        "requirements": "(CUDACapability >= 8.0)",
        "+WantGPULab": "true",
        "+GPUJobLength": '"short"',
    }

    # check if job_dir's parent is writable
    job_dir_abspath = os.path.abspath(job_dir)
    if not os.access(os.path.dirname(job_dir_abspath), os.W_OK):
        raise PermissionError(f"Job directory {job_dir} is not writable")

    # create job dir
    os.makedirs(job_dir, exist_ok=True)

    # folder to hold python code (one of the inputs to the job)
    job_src_dir = os.path.join(job_dir, 'src')
    os.makedirs(job_src_dir, exist_ok=True)

    # write KM/SKiM data to job_dir/data.tsv
    data_tsv_path = os.path.join(job_dir, 'data.tsv')
    with open(data_tsv_path, 'w') as f:
        if is_km:
            f.write('a_term\tb_term\tab_pmid_intersection\n')
            for result in data:
                f.write(f"{result['a_term']}\t{result['b_term']}\t{result['ab_pmid_intersection']}\n")
        else:
            f.write('a_term\tb_term\tc_term\tab_pmid_intersection\tbc_pmid_intersection\tac_pmid_intersection\n')
            for result in data:
                f.write(f"{result['a_term']}\t{result['b_term']}\t{result['c_term']}\t{result['ab_pmid_intersection']}\t{result['bc_pmid_intersection']}\t{result['ac_pmid_intersection']}\n")

    # write config to job_dir/config.json
    config_json_path = os.path.join(job_dir, 'config.json')
    with open(config_json_path, 'w') as f:
        json.dump(config, f, indent=4)

    # copy run.sh and relevance.py to job_dir
    cwd = os.getcwd()
    original_src_dir = os.path.join(cwd, 'src', 'condor')
    for file in ["run.sh", "relevance.py"]:
        src_path = os.path.abspath(os.path.join(original_src_dir, file))
        dst_path = os.path.join(job_dir, file)
        if os.path.exists(src_path):
            shutil.copy2(src_path, dst_path)
        else:
            raise FileNotFoundError(f"Required file {file} not found in {original_src_dir}")
            
    # copy .py files into job_dir/src
    for src_file in glob(os.path.join(original_src_dir, "*.py")):
        dst_path = os.path.join(job_src_dir, os.path.basename(src_file))
        if os.path.abspath(src_file) != os.path.abspath(dst_path):
            shutil.copy2(src_file, dst_path)
    
    # create files.txt in job_dir that contains the .tsv file name 
    with open(os.path.join(job_dir, "files.txt"), "w") as f:
        f.write(f"{os.path.basename(data_tsv_path)}\n")
    
    # navigate into job_dir. we will submit the job from here
    original_dir = os.getcwd()
    os.chdir(job_dir)

    # write the HTCondor token to a file
    token_dir = os.path.join(job_dir, "token")
    token_file = write_token_to_file(config["HTCONDOR_TOKEN"], token_dir)
    htcondor_connection_config["token_file"] = token_file

    # submit the job
    try:
        htcondor_helper = HTCondorHelper(htcondor_connection_config)
        cluster_id = htcondor_helper.submit_jobs(htcondor_job_config)
        print(f"HTCondor job submitted with cluster ID {cluster_id}")

        # monitor the job
        if htcondor_helper.monitor_jobs(cluster_id):
            print("HTCondor job completed, retrieving output...")
            htcondor_helper.retrieve_output(cluster_id)

        # read and parse the job_result.json
        print("Processing results...")
        results_json_path = os.path.join(job_dir, 'job_result.json')
        if not os.path.exists(results_json_path):
            raise FileNotFoundError(f"Results file {results_json_path} not found")
        results = parse_results(results_json_path)
    except Exception as e:
        # Ensure token is always removed even if an error occurs
        if os.path.exists(token_file):
            os.remove(token_file)
        raise e
    
    # remove token file for security reasons
    if os.path.exists(token_file):
        os.remove(token_file)

    # all done
    os.chdir(original_dir)
    print(f"Results processed successfully. Results are in {job_dir}")
    return results
    
def parse_results(result_json_path: str) -> 'list[dict]':
    parsed_results = []

    # read json
    with open(result_json_path, 'r') as f:
        results_json = json.load(f)

    for json_result in results_json:
        parsed_result = dict()

        if 'A_B_C_Relationship' in json_result: # SKiM only
            parsed_result['a_term'] = json_result['A_B_C_Relationship']['a_term']
            parsed_result['b_term'] = json_result['A_B_C_Relationship']['b_term']
            parsed_result['c_term'] = json_result['A_B_C_Relationship']['c_term']
            parsed_result['abc_result'] = json_result['A_B_C_Relationship']['Result']
        if 'A_B_Relationship' in json_result: # KM and SKiM
            parsed_result['a_term'] = json_result['A_B_Relationship']['a_term']
            parsed_result['b_term'] = json_result['A_B_Relationship']['b_term']
            parsed_result['ab_result'] = json_result['A_B_Relationship']['Result']
        if 'A_C_Relationship' in json_result: # SKiM only
            parsed_result['a_term'] = json_result['A_C_Relationship']['a_term']
            parsed_result['c_term'] = json_result['A_C_Relationship']['c_term']
            parsed_result['ac_result'] = json_result['A_C_Relationship']['Result']

        if not parsed_result:
            raise ValueError(f"Result is empty for {json_result}")

        parsed_results.append(parsed_result)

    return parsed_results

def write_token_to_file(htcondor_token: str, token_dir: str) -> str:
    """Write the HTCondor token from environment variable to the token directory"""
    # HTCondor tokens need to be in the format of a JWT with header.payload.signature
    if not htcondor_token.count('.') >= 2 and 'eyJ' not in htcondor_token:
        print("WARNING: Token doesn't appear to be in JWT format, it may not work with HTCondor")

    # HTCondor expects token files to follow a specific format
    # First try the default token file name
    os.makedirs(token_dir, exist_ok=True)
    token_file = os.path.join(token_dir, 'condor_token')
    
    with open(token_file, 'w') as f:
        f.write(htcondor_token)
    
    # Set secure permissions (owner read-only)
    os.chmod(token_file, 0o600)
    
    return token_file