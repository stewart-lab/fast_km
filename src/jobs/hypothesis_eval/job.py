import os
import json
from glob import glob
import subprocess
import sys
from rq import get_current_job
from src.fast_km_exception import FastKmException
from src.jobs.hypothesis_eval.params import HypothesisEvalJobParams, validate_params
import src.global_vars as gvars
from src.kinderminer_algorithm import kinderminer_search
from src.indexing.index import Index

IMAGE_VERSION = os.environ.get("SKIMGPT_IMAGE", "2.0.1")
SKIMGPT_IMAGE = f"docker://stewartlab/skimgpt:{IMAGE_VERSION}"

def run_hypothesis_eval_job(params: HypothesisEvalJobParams) -> dict:
    validate_params(params)

    # create the job dir to store required files
    job = get_current_job()
    if not job:
        raise RuntimeError("Could not get current RQ job")
    
    # note that this needs to be an absolute path because we change the CWD below.
    # TODO: this assumes a certain structure of the file system that is generally only true in the docker build.
    temp_dir = os.path.join("/app", "_data", "jobs", "job-" + job.id)
    os.makedirs(temp_dir, exist_ok=True)

    # run the condor job
    results = _run_skim_gpt(temp_dir, params)
    return results

def _run_skim_gpt(job_dir: str, params: HypothesisEvalJobParams) -> dict:
    """Run skimgpt-relevance in a local Docker container (Triton-first, CHTC fallback).

    Creates the job files (config.json, data.tsv, files.txt, secrets.json) in
    job_dir, then runs the SKIMGPT_IMAGE as a sibling container.  The
    container tries Triton remote inference first; if Triton is unreachable it
    falls back to submitting an HTCondor GPU job internally.
    """
    
    # Get config template
    config = _get_config_template()
    
    # Extract data and determine job type
    data = params.data
    is_km = params.KM_hypothesis is not None
    is_skim = params.SKIM_hypotheses is not None

    if not (is_km or is_skim):
        raise FastKmException('job must be KM or SKiM')
    
    data = _populate_pmid_intersections(data, params.censor_year_lower, params.censor_year_upper)

    if params.is_dch:
        config['JOB_TYPE'] = 'km_with_gpt'
        config['JOB_SPECIFIC_SETTINGS']['km_with_gpt']['is_dch'] = True
    elif is_km:
        config['JOB_TYPE'] = 'km_with_gpt'
    elif is_skim:
        config['JOB_TYPE'] = 'skim_with_gpt'
    else:
        raise FastKmException('Unable to determine job type (KM vs SKiM vs KM-DCH)')
    
    # Inject dynamic values from payload
    config['KM_hypothesis'] = params.KM_hypothesis
    config['SKIM_hypotheses'] = params.SKIM_hypotheses
    config['GLOBAL_SETTINGS']['MODEL'] = params.model
    config['GLOBAL_SETTINGS']['TOP_N_ARTICLES_MOST_CITED'] = params.top_n_articles_most_cited
    config['GLOBAL_SETTINGS']['TOP_N_ARTICLES_MOST_RECENT'] = params.top_n_articles_most_recent
    config['GLOBAL_SETTINGS']['POST_N'] = params.post_n
    # iterations==1 is "no iteration loop, write to output/" (matches the
    # legacy behaviour). >1 enables the worker's iteration loop, which writes
    # to output/iteration_{i}/ — the result collector below recurses into
    # those subdirs and tags each parsed result with its iteration index.
    config['GLOBAL_SETTINGS']['iterations'] = params.iterations if params.iterations > 1 else False
    config['JOB_SPECIFIC_SETTINGS']['km_with_gpt']['censor_year_upper'] = params.censor_year_upper
    config['JOB_SPECIFIC_SETTINGS']['km_with_gpt']['censor_year_lower'] = params.censor_year_lower
    config['JOB_SPECIFIC_SETTINGS']['skim_with_gpt']['censor_year_upper'] = params.censor_year_upper
    config['JOB_SPECIFIC_SETTINGS']['skim_with_gpt']['censor_year_lower'] = params.censor_year_lower

    # Set up secrets
    secrets = {
        "PUBMED_API_KEY": gvars.SECRET_PUBMED_API_KEY,
        "OPENAI_API_KEY": gvars.SECRET_OPENAI_API_KEY,
        "HTCONDOR_TOKEN": gvars.SECRET_HTCONDOR_TOKEN,
        "DEEPSEEK_API_KEY": gvars.SECRET_DEEPSEEK_API_KEY,
    }

    # Validate required secrets
    if not secrets['PUBMED_API_KEY']:
        raise ValueError("PUBMED_API_KEY is not set")
    if not secrets['OPENAI_API_KEY']:
        raise ValueError("OPENAI_API_KEY is not set")
    if not secrets['HTCONDOR_TOKEN']:
        raise ValueError("HTCONDOR_TOKEN is not set")
    if not secrets['DEEPSEEK_API_KEY']:
        raise ValueError("DEEPSEEK_API_KEY is not set")

    # Check if job_dir's parent is writable
    job_dir_abspath = os.path.abspath(job_dir)
    if not os.access(os.path.dirname(job_dir_abspath), os.W_OK):
        raise PermissionError(f"Job directory {job_dir} is not writable")

    # Create job directory structure
    os.makedirs(job_dir, exist_ok=True)
    os.makedirs(os.path.join(job_dir, 'output'), exist_ok=True)

    # Write data.tsv
    data_tsv_path = os.path.join(job_dir, 'data.tsv')
    with open(data_tsv_path, 'w') as f:
        if is_km:
            f.write('a_term\tb_term\tab_pmid_intersection\n')
            for item in data:
                ab_pmid_list = [str(p) for p in item['ab_pmid_intersection']]
                ab_pmids_str = str(ab_pmid_list)
                f.write(f"{item['a_term']}\t{item['b_term']}\t{ab_pmids_str}\n")
        else:
            f.write('a_term\tb_term\tc_term\tab_pmid_intersection\tbc_pmid_intersection\tac_pmid_intersection\n')
            for item in data:
                ab_pmid_list = [str(p) for p in item['ab_pmid_intersection']]
                bc_pmid_list = [str(p) for p in item['bc_pmid_intersection']]
                ac_pmid_list = [str(p) for p in item['ac_pmid_intersection']]
                ab_pmids_str = str(ab_pmid_list)
                bc_pmids_str = str(bc_pmid_list)
                ac_pmids_str = str(ac_pmid_list)
                f.write(f"{item['a_term']}\t{item['b_term']}\t{item['c_term']}\t{ab_pmids_str}\t{bc_pmids_str}\t{ac_pmids_str}\n")

    # Write files.txt (points to the data file)
    files_txt_path = os.path.join(job_dir, 'files.txt')
    with open(files_txt_path, 'w') as f:
        f.write(f"{os.path.basename(data_tsv_path)}\n")

    # Write config.json
    config_json_path = os.path.join(job_dir, 'config.json')
    with open(config_json_path, 'w') as f:
        json.dump(config, f, indent=4)

    # Write secrets.json
    secrets_json_path = os.path.join(job_dir, "secrets.json")
    with open(secrets_json_path, "w") as f:
        json.dump(secrets, f, indent=4)

    # pull SKiM-GPT image if it's not present
    sif_image_dir = os.path.join("/app", "_data", "images")
    os.makedirs(sif_image_dir, exist_ok=True)
    safe_name = SKIMGPT_IMAGE.replace("docker://", "").replace("/", "_").replace(":", "_").replace(".", "_")
    local_image_path = os.path.join(sif_image_dir, f"{safe_name}.sif")
    if not os.path.exists(local_image_path):
        print(f"Pulling {SKIMGPT_IMAGE} to {local_image_path}...")
        tmp_path = os.path.join(job_dir, f"{safe_name}.sif") # avoids race conditions if two jobs try to pull the image at the same time
        proc = subprocess.Popen([
            "apptainer", 
            "pull", 
            tmp_path,
            SKIMGPT_IMAGE
        ], 
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,    # merge stderr into stdout so we can see all output
        text=True,
        bufsize=1,                   # line-buffered
        )

        for line in proc.stdout:
            print(line, end="")

        exit_code = proc.wait()
        if exit_code != 0:
            raise RuntimeError(f"skimgpt-relevance container pull failed with code {exit_code}")
        
        os.rename(tmp_path, local_image_path)

    # spawn a child container (using apptainer) to run the hypothesis evaluation pipeline
    print(f"Running {SKIMGPT_IMAGE}...")

    proc = subprocess.Popen([
        "apptainer", 
        "exec", 
        "--bind", f"{job_dir}:{job_dir}",
        "--cwd", job_dir,
        "--cleanenv",
        "--env", f"PYTHONUNBUFFERED=1,HTCONDOR_TOKEN={secrets['HTCONDOR_TOKEN']}",
        local_image_path,
        "skimgpt-relevance",
        "--km_output", files_txt_path,
        "--config", config_json_path
    ], 
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,    # merge stderr into stdout so order is preserved
    text=True,
    bufsize=1,                   # line-buffered
    )

    for line in proc.stdout:
        print(line, end="")

    exit_code = proc.wait()
    if exit_code != 0:
        raise RuntimeError(f"skimgpt-relevance container exited with code {exit_code}")

    # Collect result JSON files from both possible output locations:
    #   - Triton success: written to job_dir root by run_relevance_pipeline
    #   - CHTC fallback:  written to job_dir/output/ by HTCondor transfer
    # When iterations > 1 the worker writes to output/iteration_{i}/ — recurse
    # so per-iteration JSONs are picked up.
    print("Processing results...")
    skip = {"config.json", "secrets.json"}
    result_files = [
        f for f in glob(os.path.join(job_dir, "*.json"))
        if os.path.basename(f) not in skip
    ]
    output_dir = os.path.join(job_dir, "output")
    if os.path.exists(output_dir):
        result_files.extend(glob(os.path.join(output_dir, "**", "*.json"), recursive=True))

    if not result_files:
        for dirpath, _, filenames in os.walk(job_dir):
            for fn in filenames:
                print(f"  {os.path.relpath(os.path.join(dirpath, fn), job_dir)}")
        raise FileNotFoundError(f"No result JSON files found in {job_dir}")

    print(f"Found {len(result_files)} result file(s): {', '.join(result_files)}")
    results = []
    for result_file in result_files:
        iter_idx = _iteration_from_path(result_file)
        for parsed in _parse_json_result(result_file):
            if iter_idx is not None:
                parsed['iteration'] = iter_idx
            results.append(parsed)

    print(f"Results processed successfully. Job files are in {job_dir}")
    return results


def _iteration_from_path(path: str) -> int | None:
    """Extract the iteration index from a per-iteration result path.

    The worker writes to ``output/iteration_{i}/…json`` when the iteration
    loop is enabled. Returns ``None`` for files outside an ``iteration_*``
    directory (i.e. the legacy single-run layout).
    """
    parent = os.path.basename(os.path.dirname(path))
    if parent.startswith("iteration_"):
        try:
            return int(parent.split("_", 1)[1])
        except ValueError:
            return None
    return None

def _parse_json_result(json_result_file: str) -> list[dict]:
    parsed_results = []

    with open(json_result_file, 'r') as f:
        results_json = json.load(f)

    if not isinstance(results_json, list):
        results_json = [results_json]

    for json_result in results_json:
        parsed_result = dict()

        if 'Hypothesis_Comparison' in json_result:
            hc = json_result['Hypothesis_Comparison']
            parsed_result['hypothesis1'] = hc.get('hypothesis1', '')
            parsed_result['hypothesis2'] = hc.get('hypothesis2', '')
            results_list = hc.get('Result', [])
            if not isinstance(results_list, list):
                results_list = [results_list]
            for result_entry in results_list:
                if not isinstance(result_entry, dict):
                    continue
                parsed_result['score'] = result_entry.get('score')
                parsed_result['decision'] = result_entry.get('decision')
                tallies = result_entry.get('tallies', {}) or {}
                parsed_result['support_H1'] = tallies.get('support_H1', 0)
                parsed_result['support_H2'] = tallies.get('support_H2', 0)
                parsed_result['both'] = tallies.get('both', 0)
                parsed_result['neither_or_inconclusive'] = tallies.get('neither_or_inconclusive', 0)
            parsed_result['total_relevant_abstracts'] = json_result.get('total_relevant_abstracts')
        elif 'A_B_C_Relationship' in json_result:
            parsed_result['a_term'] = json_result['A_B_C_Relationship']['a_term']
            parsed_result['b_term'] = json_result['A_B_C_Relationship']['b_term']
            parsed_result['c_term'] = json_result['A_B_C_Relationship']['c_term']
            parsed_result['abc_result'] = json_result['A_B_C_Relationship']['Result']
        if 'A_B_Relationship' in json_result:
            parsed_result['a_term'] = json_result['A_B_Relationship']['a_term']
            parsed_result['b_term'] = json_result['A_B_Relationship']['b_term']
            parsed_result['ab_result'] = json_result['A_B_Relationship']['Result']
        if 'A_C_Relationship' in json_result:
            parsed_result['a_term'] = json_result['A_C_Relationship']['a_term']
            parsed_result['c_term'] = json_result['A_C_Relationship']['c_term']
            parsed_result['ac_result'] = json_result['A_C_Relationship']['Result']

        parsed_result['original_json'] = json_result

        if not parsed_result:
            raise ValueError(f"Result is empty for {json_result}")

        parsed_results.append(parsed_result)

    return parsed_results


def _get_config_template():
    """Get the static configuration template that matches kmGPT's expected schema"""
    return {
        "JOB_TYPE": "km_with_gpt",
        "KM_hypothesis": "",
        "SKIM_hypotheses": {
            "BC": "",
            "AB": "",
            "rel_AC": "",
            "AC": "",
            "ABC": ""
        },
        "Evaluate_single_abstract": False,
        "GLOBAL_SETTINGS": {
            "A_TERM": "",
            "A_TERM_SUFFIX": "",
            "TOP_N_ARTICLES_MOST_CITED": 50,
            "TOP_N_ARTICLES_MOST_RECENT": 50,
            "POST_N": 5,
            "MIN_WORD_COUNT": 98,
            "MODEL": "r1",
            "API_URL": "http://localhost:5099/skim/api/jobs",
            "PORT": "5081",
            "RATE_LIMIT": 3,
            "DELAY": 10,
            "MAX_RETRIES": 10,
            "RETRY_DELAY": 5,
            "LOG_LEVEL": "INFO",
            "OUTDIR_SUFFIX": "",
            "iterations": False,
            "DCH_MIN_SAMPLING_FRACTION": 0.06,
            "DCH_SAMPLE_SIZE": 50,
            "TRITON_MAX_WORKERS": 10,
            "TRITON_SHOW_PROGRESS": True,
            "TRITON_BATCH_CHUNK_SIZE": None
        },
        "HTCONDOR": {
            "collector_host": "cm.chtc.wisc.edu",
            "submit_host": "ap2002.chtc.wisc.edu",
            "docker_image": f"{SKIMGPT_IMAGE}",
            "request_gpus": "1",
            "request_cpus": "1",
            "request_memory": "15GB",
            "request_disk": "15GB"
        },
        "relevance_filter": {
            "SERVER_URL": "https://xdddev.chtc.io/triton",
            "MODEL_NAME": "porpoise",
            "MODEL": "lexu14/porpoise1",
            "TEMPERATURE": 0,
            "TOP_K": 20,
            "TOP_P": 0.95,
            "MAX_COT_TOKENS": 500,
            "DEBUG": False,
            "TEST_LEAKAGE": False,
            "TEST_LEAKAGE_TYPE": "empty"
        },
        "JOB_SPECIFIC_SETTINGS": {
            "km_with_gpt": {
                "position": False,
                "A_TERM_LIST": False,
                "A_TERMS_FILE": "",
                "B_TERMS_FILE": "",
                "is_dch": False,
                "SORT_COLUMN": "ab_sort_ratio",
                "NUM_B_TERMS": 25,
                "ab_fet_threshold": 1,
                "censor_year_upper": gvars.MAX_CENSOR_YEAR,
                "censor_year_lower": gvars.MIN_CENSOR_YEAR
            },
            "skim_with_gpt": {
                "position": True,
                "A_TERM_LIST": True,
                "A_TERMS_FILE": "",
                "B_TERMS_FILE": "",
                "NUM_B_TERMS": 20000,
                "C_TERMS_FILE": "",
                "SORT_COLUMN": "bc_sort_ratio",
                "ab_fet_threshold": 1,
                "bc_fet_threshold": 1,
                "censor_year_upper": gvars.MAX_CENSOR_YEAR,
                "censor_year_lower": gvars.MIN_CENSOR_YEAR
            }
        }
    }

def _populate_pmid_intersections(data: list[dict], censor_year_lower: int, censor_year_upper: int) -> list[dict]:
    """Run KinderMiner searches to populate ab_pmid_intersection for each data entry.

    Used by DCH jobs where the caller provides empty PMID lists and expects
    fast_km to compute the full A-B PMID intersection internally.
    Delegates to run_kinderminer_job which handles Index lifecycle and searching.
    """
    # data structure example:

    # KM:
    # data = [
    #     { 
    #         'a_term': 'breast cancer', 
    #         'b_term': 'ABEMACICLIB', 
    #         'ab_pmid_intersection': ['28580882', '28968163', '31250942', '33029704', '32955138']
    #     },
    # ]

    # SKiM:
    # data = [
    #     { 
    #         'a_term': 'breast cancer', 
    #         'b_term': 'CDK4', 
    #         'c_term': 'ABEMACICLIB',
    #         'ab_pmid_intersection': ['26030518', '19874578', '32940689', '33260316', '30130984'],
    #         'bc_pmid_intersection': ['27030077', '27217383', '34657059', '34958115', '37382948'],
    #         'ac_pmid_intersection': ['28580882', '28968163', '31250942', '33029704', '32955138']
    #     },
    # ]

    idx = Index(gvars.data_dir)

    for result in data:
        a_term = result["a_term"]
        b_term = result["b_term"]
        c_term = result.get("c_term")

        # populate PMID intersections if not provided by the user
        if not result.get("ab_pmid_intersection"):
            result["ab_pmid_intersection"] = kinderminer_search(idx, 
                                                                a_term=a_term, 
                                                                b_term=b_term, 
                                                                c_term=None,
                                                                censor_year_lower=censor_year_lower, 
                                                                censor_year_upper=censor_year_upper,
                                                                return_pmids=True, 
                                                                top_n_articles_most_recent=1000)["ab_pmid_intersection"]
        if c_term and not result.get("bc_pmid_intersection"):
            result["bc_pmid_intersection"] = kinderminer_search(idx, 
                                                                a_term=None,
                                                                b_term=b_term, 
                                                                c_term=c_term, 
                                                                censor_year_lower=censor_year_lower, 
                                                                censor_year_upper=censor_year_upper,
                                                                return_pmids=True, 
                                                                top_n_articles_most_recent=1000)["bc_pmid_intersection"]
        if c_term and not result.get("ac_pmid_intersection"):
            result["ac_pmid_intersection"] = kinderminer_search(idx, 
                                                                a_term=a_term, 
                                                                b_term=None,
                                                                c_term=c_term, 
                                                                censor_year_lower=censor_year_lower, 
                                                                censor_year_upper=censor_year_upper,
                                                                return_pmids=True, 
                                                                top_n_articles_most_recent=1000)["ac_pmid_intersection"]

    idx.close()
    return data