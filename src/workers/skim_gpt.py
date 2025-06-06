import os
import shutil
import json
from glob import glob
from condor.htcondor_helper import HTCondorHelper
from condor.utils import Config
import indexing.km_util as km_util

def get_static_config_template():
    """Get the static configuration template that works with skimgpt"""
    return {
        "JOB_TYPE": "km_with_gpt",  # Will be overridden based on data structure
        "KM_hypothesis": "",  # Will be injected from payload
        "KM_direct_comp_hypothesis": "",
        "SKIM_hypotheses": {},
        "Evaluate_single_abstract": False,
        "GLOBAL_SETTINGS": {
            "A_TERM": "",
            "A_TERM_SUFFIX": "",
            "TOP_N_ARTICLES_MOST_CITED": 50,  # Will be injected from payload
            "TOP_N_ARTICLES_MOST_RECENT": 50,  # Will be injected from payload
            "POST_N": 5,
            "MIN_WORD_COUNT": 98,
            "MODEL": "r1",  # Will be injected from payload
            "API_URL": "http://localhost:5099/skim/api/jobs",
            "PORT": "5081",
            "RATE_LIMIT": 3,
            "DELAY": 10,
            "MAX_RETRIES": 10,
            "RETRY_DELAY": 5,
            "LOG_LEVEL": "INFO",
            "OUTDIR_SUFFIX": "",
            "iterations": False
        },
        "HTCONDOR": {
            "collector_host": "cm.chtc.wisc.edu",
            "submit_host": "ap2002.chtc.wisc.edu",
            "docker_image": "docker://stewartlab/skimgpt:0.1.6",
            "request_gpus": "1",
            "request_cpus": "1",
            "request_memory": "24GB",
            "request_disk": "60GB"
        },
        "abstract_filter": {
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
                "NUM_B_TERMS": 25,
                "km_with_gpt": {
                    "ab_fet_threshold": 1,
                    "censor_year": 2024
                }
            },
            "km_with_gpt_direct_comp": {
                "position": False,
                "A_TERM_LIST": False,
                "A_TERMS_FILE": "",
                "B_TERMS_FILE": "",
                "SORT_COLUMN": "ab_sort_ratio",
                "NUM_B_TERMS": 25,
                "km_with_gpt_direct_comp": {
                    "ab_fet_threshold": 1,
                    "censor_year": 1990
                }
            },
            "skim_with_gpt": {
                "position": True,
                "A_TERM_LIST": True,
                "A_TERMS_FILE": "",
                "B_TERMS_FILE": "",
                "NUM_B_TERMS": 20000,
                "C_TERMS_FILE": "",
                "SORT_COLUMN": "bc_pvalue",
                "skim_with_gpt": {
                    "ab_fet_threshold": 1,
                    "bc_fet_threshold": 1,
                    "censor_year": 2025,
                    "top_n": 300
                }
            }
        }
    }

def run_skim_gpt(job_dir: str, payload: dict) -> dict:
    """
    Submit a SKiM-GPT or KM-GPT job to HTCondor and return the parsed results.
    
    Args:
        job_dir: Directory to create job files in
        payload: Request payload with structure like:
        {
            "model": "r1",
            "KM_hypothesis": "{a_term} (Disease or Condition) informs {b_term} (Drug or Compound)",
            "top_n_articles_most_cited": 5,
            "top_n_articles_most_recent": 6,
            "data": [{"a_term": "...", "b_term": "...", "ab_pmid_intersection": [...]}]
        }
    
    Returns:
        Dictionary where keys are result filenames (without .json extension) and values are the parsed JSON content.
        Structure will vary based on job type (KM vs SKiM) but typically includes relationship analysis results.
        
        Example structure for KM jobs:
        {
            "irritability_AES_km_with_gpt": {
                "A_B_C_Relationship": {...},
                "A_C_Relationship": {...},
                "ab_relevance": "0.89 (89/100)",
                ...
            }
        }
    """
    
    # Get static config template
    config = get_static_config_template()
    
    # Extract data and determine job type
    data = payload['data']
    is_km = 'c_term' not in data[0]
    is_direct_comp = 'KM_direct_comp_hypothesis' in payload
    
    # Set job type
    if is_direct_comp:
        config['JOB_TYPE'] = 'km_with_gpt_direct_comp'
        config['KM_direct_comp_hypothesis'] = payload.get('KM_direct_comp_hypothesis', '')
    elif is_km:
        config['JOB_TYPE'] = 'km_with_gpt'
    else:
        config['JOB_TYPE'] = 'skim_with_gpt'
    
    # Inject dynamic values from payload
    config['KM_hypothesis'] = payload.get('KM_hypothesis', '')
    config['SKIM_hypotheses'] = payload.get('SKIM_hypotheses', {})
    config['GLOBAL_SETTINGS']['MODEL'] = payload.get('model', 'r1')
    config['GLOBAL_SETTINGS']['TOP_N_ARTICLES_MOST_CITED'] = payload.get('top_n_articles_most_cited', 50)
    config['GLOBAL_SETTINGS']['TOP_N_ARTICLES_MOST_RECENT'] = payload.get('top_n_articles_most_recent', 50)
    
    # Set up secrets
    secrets = {
        "PUBMED_API_KEY": km_util.pubmed_api_key,
        "OPENAI_API_KEY": km_util.openai_api_key,
        "HTCONDOR_TOKEN": km_util.htcondor_token,
        "DEEPSEEK_API_KEY": getattr(km_util, 'deepseek_api_key', ''),
    }

    # Validate required secrets
    if not secrets['PUBMED_API_KEY']:
        raise ValueError("PUBMED_API_KEY is not set")
    if not secrets['OPENAI_API_KEY']:
        raise ValueError("OPENAI_API_KEY is not set")
    if not secrets['HTCONDOR_TOKEN']:
        raise ValueError("HTCONDOR_TOKEN is not set")

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
                # Convert PMID intersection to Python list representation with strings
                pmids = item['ab_pmid_intersection']
                
                # Handle different input formats
                if isinstance(pmids, str):
                    # If it's a string representation of a list, evaluate it
                    import ast
                    try:
                        pmids = ast.literal_eval(pmids)
                    except:
                        # If not a valid list string, assume it's comma-separated
                        pmids = [p.strip() for p in pmids.split(',') if p.strip()]
                
                # Ensure we have a list
                if not isinstance(pmids, list):
                    pmids = list(pmids) if pmids else []
                
                # Convert to Python list representation with strings
                pmids_list = [str(p) for p in pmids]
                pmids_str = str(pmids_list)
                f.write(f"{item['a_term']}\t{item['b_term']}\t{pmids_str}\n")
        else:
            f.write('a_term\tb_term\tc_term\tab_pmid_intersection\tbc_pmid_intersection\tac_pmid_intersection\n')
            for item in data:
                # Convert all PMID intersections to Python list representation with strings
                def convert_pmids(pmids):
                    # Handle different input formats
                    if isinstance(pmids, str):
                        import ast
                        try:
                            pmids = ast.literal_eval(pmids)
                        except:
                            pmids = [p.strip() for p in pmids.split(',') if p.strip()]
                    
                    # Ensure we have a list
                    if not isinstance(pmids, list):
                        pmids = list(pmids) if pmids else []
                    
                    # Convert to Python list representation with strings
                    pmids_list = [str(p) for p in pmids]
                    return str(pmids_list)
                
                ab_pmids = convert_pmids(item['ab_pmid_intersection'])
                bc_pmids = convert_pmids(item['bc_pmid_intersection'])
                ac_pmids = convert_pmids(item['ac_pmid_intersection'])
                f.write(f"{item['a_term']}\t{item['b_term']}\t{item['c_term']}\t{ab_pmids}\t{bc_pmids}\t{ac_pmids}\n")

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

    # Copy run.sh executable
    condor_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'condor')
    src_run_sh = os.path.join(condor_dir, "run.sh")
    dst_run_sh = os.path.join(job_dir, "run.sh")
    if os.path.exists(src_run_sh):
        shutil.copy2(src_run_sh, dst_run_sh)
        os.chmod(dst_run_sh, 0o755)  # Make executable
    else:
        raise FileNotFoundError(f"Required file run.sh not found in {condor_dir}")
    
    # Navigate to job directory for submission
    original_dir = os.getcwd()
    os.chdir(job_dir)

    try:
        # Write HTCondor token to file
        token_dir = os.path.join(job_dir, "token")
        token_file = write_token_to_file(secrets["HTCONDOR_TOKEN"], token_dir)

        # Create Config object that HTCondorHelper expects
        # Pass the config file path directly, not an Args object
        config_obj = Config(config_json_path)

        # Load the TSV data into the config object
        config_obj.load_km_output(files_txt_path)

        # Create HTCondor helper and submit job
        htcondor_helper = HTCondorHelper(config_obj, token_dir)
        cluster_id = htcondor_helper.submit_jobs(files_txt_path)
        print(f"HTCondor job submitted with cluster ID {cluster_id}")

        # Function to stream log files during monitoring
        def stream_log_files():
            """Copy HTCondor log files to job directory during monitoring"""
            # Check for log files in multiple locations
            log_patterns = [
                f"run_{cluster_id}.log",      # In root job directory
                "output/std_out.out",         # In output subdirectory  
                "output/std_err.err",         # In output subdirectory
                "std_out.out",                # Also check root just in case
                "std_err.err"                 # Also check root just in case
            ]
            
            for pattern in log_patterns:
                log_file = pattern  # pattern is already a path, not a glob pattern
                if os.path.exists(log_file):
                    log_dst = os.path.join(job_dir, os.path.basename(log_file))
                    # Only copy if source and destination are different and source is newer
                    if os.path.abspath(log_file) != os.path.abspath(log_dst):
                        try:
                            # Check if we need to update the file
                            if not os.path.exists(log_dst) or os.path.getmtime(log_file) > os.path.getmtime(log_dst):
                                shutil.copy2(log_file, log_dst)
                                print(f"Streamed {log_file} to {log_dst}")
                        except Exception as e:
                            # Don't fail monitoring if log copying fails
                            print(f"Warning: Could not stream {log_file}: {e}")

        # Monitor job completion with log streaming
        print("Monitoring job with log streaming...")
        monitoring_success = False
        try:
            # Start monitoring in a separate thread while streaming logs
            import threading
            import time
            
            # Monitor the job
            def monitor_job():
                nonlocal monitoring_success
                monitoring_success = htcondor_helper.monitor_jobs(cluster_id)
            
            # Start monitoring thread
            monitor_thread = threading.Thread(target=monitor_job)
            monitor_thread.daemon = True
            monitor_thread.start()
            
            # Stream log files while monitoring
            while monitor_thread.is_alive():
                stream_log_files()
                time.sleep(10)  # Stream logs every 10 seconds
                monitor_thread.join(timeout=0)  # Check if thread is done
            
            # Final log streaming after monitoring completes
            stream_log_files()
            
        except Exception as e:
            print(f"Error during monitoring: {e}")
            monitoring_success = False

        if monitoring_success:
            print("HTCondor job completed, retrieving output...")
            htcondor_helper.retrieve_output(cluster_id)

            # Final log file streaming after completion
            stream_log_files()

        # Parse results - look for result files in output directory only
        print("Processing results...")
        
        # Look for result files in output directory with actual naming patterns
        output_dir = os.path.join(job_dir, 'output')
        result_files = []
        
        if os.path.exists(output_dir):
            # Look for job-specific result files (e.g., irritability_AES_km_with_gpt.json)
            result_patterns = [
                '*_km_with_gpt.json',
                '*_km_with_gpt_direct_comp.json', 
                '*_skim_with_gpt.json'
            ]
            
            for pattern in result_patterns:
                pattern_files = glob(os.path.join(output_dir, pattern))
                result_files.extend(pattern_files)
            
            # Remove duplicates while preserving order
            seen = set()
            result_files = [f for f in result_files if not (f in seen or seen.add(f))]
                
        if result_files:
            print(f"Found {len(result_files)} result file(s):")
            for rf in result_files:
                rel_path = os.path.relpath(rf, job_dir)
                print(f"  {rel_path}")
            
            # Parse the JSON content from each result file
            results = {}
            for i, result_file in enumerate(result_files):
                try:
                    with open(result_file, 'r') as f:
                        content = json.load(f)
                    
                    # Use filename (without path and extension) as the key
                    filename = os.path.basename(result_file)
                    key = os.path.splitext(filename)[0]  # Remove .json extension
                    
                    # If content is a list (like the example), take the first item
                    # If it's already a dict, use it directly
                    if isinstance(content, list) and len(content) > 0:
                        results[key] = content[0]
                    elif isinstance(content, dict):
                        results[key] = content
                    else:
                        # Fallback: store the raw content
                        results[key] = content
                        
                    print(f"✓ Parsed {filename}")
                    
                except Exception as e:
                    print(f"✗ Error reading {result_file}: {e}")
                    # Store file path as fallback  
                    filename = os.path.basename(result_file)
                    key = os.path.splitext(filename)[0]
                    results[key] = {"error": f"File reading failed: {e}", "file_path": result_file}
        else:
            # List what files we do have for debugging
            print(f"No result files found. Files in output directory:")
            if os.path.exists(output_dir):
                for file in os.listdir(output_dir):
                    print(f"  output/{file}")
            else:
                print("  output/ directory does not exist")
            raise FileNotFoundError(f"No result files found in {output_dir}")
        
        # Cleanup HTCondor job
        htcondor_helper.cleanup(cluster_id)
        
    finally:
        # Always cleanup token file and return to original directory
        if 'token_file' in locals() and os.path.exists(token_file):
            os.remove(token_file)
    os.chdir(original_dir)

    print(f"Results processed successfully. Job files are in {job_dir}")
    return results

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