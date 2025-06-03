import os
import shutil
import json
from glob import glob
import src
from src.htcondor_helper import HTCondorHelper
from src.utils import Config
import indexing.km_util as km_util

def run_skim_gpt(job_dir: str, config: dict) -> 'list[dict]':
    """
    Submit a SKiM-GPT or KM-GPT job to HTCondor and return the results.
    """

    data = config['data']
    is_km = 'c_term' not in data[0]
    is_direct_comp = 'KM_direct_comp_hypothesis' in config

    # fill in remainder of config
    if is_direct_comp:
        config['JOB_TYPE'] = 'km_with_gpt_direct_comp'
        config.setdefault('KM_hypothesis', '')
        config.setdefault('SKIM_hypotheses', {})
    elif is_km:
        config['JOB_TYPE'] = 'km_with_gpt'
        config.setdefault('SKIM_hypotheses', {})
        config.setdefault("KM_direct_comp_hypothesis", "")
    else:
        config['JOB_TYPE'] = 'skim_with_gpt'
        config.setdefault('KM_hypothesis', '')
        config.setdefault("KM_direct_comp_hypothesis", "")

    config['Evaluate_single_abstract'] = False

    config['GLOBAL_SETTINGS'] = {
        "A_TERM": "",
        "A_TERM_SUFFIX": "",
        "TOP_N_ARTICLES_MOST_CITED": 50,
        "TOP_N_ARTICLES_MOST_RECENT": 50,
        "POST_N": 5,
        "MIN_WORD_COUNT": 98,
        "MODEL": config.get('model', 'r1'),     # default to deepseek r1 model, TODO: make the default model an environment variable
        "API_URL": "http://localhost:5099/skim/api/jobs",
        "PORT": "5081",
        "RATE_LIMIT": 3,
        "DELAY": 10,
        "MAX_RETRIES": 10,
        "RETRY_DELAY": 5,
        "LOG_LEVEL": "INFO"
    }

    config['GLOBAL_SETTINGS']['OUTDIR_SUFFIX'] = ""

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

    config['PUBMED_API_KEY'] = km_util.pubmed_api_key
    config['API_KEY'] = km_util.openai_api_key
    config['HTCONDOR_TOKEN'] = km_util.htcondor_token
    config['DEEPSEEK_API_KEY'] = getattr(km_util, 'deepseek_api_key', '')

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
        "docker_image": "docker://stewartlab/skimgpt:0.1.6",
        "docker_pull_policy": "missing",
        
        # this transfers files from the submit node to the execute node
        # the 'spooling' stuff below transfers the files from the current machine to the submit node
        'should_transfer_files': 'YES',
        'transfer_input_files': '.', 

        # this command is run on the execute node
        'executable': 'run.sh',

        # execute node logging
        'log': 'run_$(Cluster).log',
        'error': 'output/std_err.err',
        'output': 'output/std_out.out',
        "stream_error": "true",
        "stream_output": "true",

        # this transfers files from the execute node to the submit node
        # the 'retrieve_output' in htcondor_helper.py transfers the files from the submit node to the current machine
        "transfer_output_files": "output",
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

    # write secrets to job_dir/secrets.json (include DEEPSEEK_API_KEY)
    secrets = {
        "PUBMED_API_KEY":    config["PUBMED_API_KEY"],
        "OPENAI_API_KEY":    config["API_KEY"],
        "HTCONDOR_TOKEN":    config["HTCONDOR_TOKEN"],
        "DEEPSEEK_API_KEY":  config["DEEPSEEK_API_KEY"],
    }
    secrets_json_path = os.path.join(job_dir, "secrets.json")
    with open(secrets_json_path, "w") as f:
        json.dump(secrets, f, indent=4)

    # remove secrets from config to avoid including them in config.json
    config.pop("PUBMED_API_KEY")
    config.pop("API_KEY")
    config.pop("HTCONDOR_TOKEN")
    config.pop("DEEPSEEK_API_KEY")

    # Embed HTCondor settings in the format skimgpt.Config expects
    config["HTCONDOR"] = {
        "collector_host": htcondor_connection_config["collector_host"],
        "submit_host":   htcondor_connection_config["submit_host"],
        "docker_image":  htcondor_job_config["docker_image"],
        "request_gpus":  htcondor_job_config["request_gpus"],
        "request_cpus":  htcondor_job_config["request_cpus"],
        "request_memory":htcondor_job_config["request_memory"],
        "request_disk":  htcondor_job_config["request_disk"],
    }

    # write config to job_dir/config.json
    config_json_path = os.path.join(job_dir, 'config.json')
    with open(config_json_path, 'w') as f:
        json.dump(config, f, indent=4)

    # copy run.sh and relevance.py from installed skimgpt package
    pkg_dir = os.path.dirname(src.__file__)
    for file in ["run.sh", "relevance.py"]:
        src_path = os.path.join(pkg_dir, file)
        dst_path = os.path.join(job_dir, file)
        if os.path.exists(src_path):
            shutil.copy2(src_path, dst_path)
        else:
            raise FileNotFoundError(f"Required file {file} not found in {pkg_dir}")
            
    # copy .py files into job_dir/src
    for src_file in glob(os.path.join(pkg_dir, "*.py")):
        dst_path = os.path.join(job_src_dir, os.path.basename(src_file))
        if os.path.abspath(src_file) != os.path.abspath(dst_path):
            shutil.copy2(src_file, dst_path)
    
    # create files.txt in job_dir that contains the .tsv file name 
    with open(os.path.join(job_dir, "files.txt"), "w") as f:
        f.write(f"{os.path.basename(data_tsv_path)}\n")
    
    # navigate into job_dir. we will submit the job from here
    original_dir = os.getcwd()
    os.chdir(job_dir)

    # write the HTCondor token to a file (now pulled from our secrets dict)
    token_dir = os.path.join(job_dir, "token")
    token_file = write_token_to_file(secrets["HTCONDOR_TOKEN"], token_dir)
    htcondor_connection_config["token_file"] = token_file  # retained if used elsewhere

    # instantiate Config (reads config.json + secrets.json)
    config_obj = Config(config_json_path)

    # submit the job via the skimgpt package's HTCondorHelper
    try:
        htcondor_helper = HTCondorHelper(config_obj, token_dir)
        # submit_jobs now takes the path to files.txt (we've chdir'ed into job_dir)
        cluster_id = htcondor_helper.submit_jobs("files.txt")
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