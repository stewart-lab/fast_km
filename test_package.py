#!/usr/bin/env python3
"""
test_package.py

This script will:
1. Send a POST request to the /hypothesis_eval/api/jobs/ endpoint of our local Flask server
   to submit a new KM hypothesis-scoring job with a sample payload.
2. After enqueuing the job, it prints the job ID and the path to the temporary job directory
   where run_skim_gpt writes data.tsv, config.json, and files.txt.
3. Wait a short moment to allow run_skim_gpt to create those files, then read and print:
   - data.tsv (the raw TSV of PMIDs & terms)
   - config.json (the full JSON config dict, including your hypothesis templates and defaults)
4. Poll the GET endpoint /hypothesis_eval/api/jobs/?id=<job_id> until the job status is 'finished',
   then print the final result returned by the server.
"""

import time
import json
import os
import requests
from datetime import datetime

# Base URL for the API. Reads FLASK_PORT env var (default 5000) to match server's listening port.
port = os.environ.get("FLASK_PORT", "5000")
API_BASE = f"http://localhost:{port}"
POST_URL = f"{API_BASE}/hypothesis_eval/api/jobs/"
GET_URL  = f"{API_BASE}/hypothesis_eval/api/jobs/"

# Sample payload matching the expected schema for a KM job
test_payload = {
    "model": "r1",
    "KM_hypothesis": "{a_term} (Disease or Condition) informs {b_term} (Drug or Compound)",
    "top_n_articles_most_cited": 5,
    "top_n_articles_most_recent": 6,
    "data": [
        {
            "a_term": "irritability",
            "b_term": "AES",
            "ab_pmid_intersection": [
                19948625,17577466,17028508,19508731,23772853,28616094,24867391,25963890,10448828,
                19877974,27521586,24204744,22440147,26362377,18162245,28364655,18806238,20435238,
                30506559,27183299,19877973,29414548,23607817,19209095,24138011,24350813,26057204,
                28830031,21186969,22174029,31370779,18254023,28557548,30694697,23973321,23388195,
                31937513,26293742,28826192,31812902,22327493,30169997,27727408,22780580,32179501,
                30796634,31371205,25424932,22173143,30919267,28616427,31145316,33090921,31295004,
                31194712,25830489,27343983,31634044,34127116,28217821,30431383,30431387,17548138,
                31841646,34112520,34427754,34075567,29804542,24918561,34951748,36001871,34131547,
                34785411,34307747,35677871,35025076,35870432,32048889,27443451,27494163,34477852,
                32273185,35777333,35232596,35816832,35813250,35906006
            ]
        }
    ]
}

def main():
    # 1) Submit the job via POST
    print("Submitting POST to", POST_URL)
    response = requests.post(POST_URL, json=test_payload)
    response.raise_for_status()
    resp_json = response.json()
    job_id = resp_json.get("id")
    print("Received job ID:", job_id)

    # 2) Derive the temp job directory
    job_dir = f"/tmp/job-{job_id}"
    print("Expecting job files in:", job_dir)

    # prepare a timestamped output subdir under fast_km/tests
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_data_dir = os.path.abspath(os.path.join(script_dir, "tests"))
    os.makedirs(test_data_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(test_data_dir, timestamp)
    os.makedirs(out_dir, exist_ok=True)
    print(f"Saving test outputs to {out_dir}")

    # 3) Wait for run_skim_gpt to create data.tsv, config.json, and secrets.json
    data_tsv_path = os.path.join(job_dir, "data.tsv")
    config_json_path = os.path.join(job_dir, "config.json")
    secrets_json_path = os.path.join(job_dir, "secrets.json")
    timeout = 60  # seconds
    start_time = time.time()
    while True:
        missing = [p for p in (data_tsv_path, config_json_path, secrets_json_path) if not os.path.exists(p)]
        if not missing:
            break
        if time.time() - start_time > timeout:
            print(f"Timeout ({timeout}s) waiting for files: {', '.join(missing)}")
            break
        time.sleep(1)
    print("All required job files are present (or timeout reached).")

    # 4) Read, print, and save data.tsv
    data_tsv_path = os.path.join(job_dir, "data.tsv")
    print(f"\n--- Contents of {data_tsv_path} ---")
    try:
        with open(data_tsv_path) as f:
            data_content = f.read()
        print(data_content)

        # Save data.tsv into our timestamped subdir
        data_dst = os.path.join(out_dir, "data.tsv")
        with open(data_dst, "w") as out_f:
            out_f.write(data_content)
        print(f"Saved data TSV to {data_dst}")
    except FileNotFoundError:
        print("data.tsv not found; run_skim_gpt may not have completed yet.")

    # 5) Read, print, and save config.json
    config_json_path = os.path.join(job_dir, "config.json")
    print(f"\n--- Contents of {config_json_path} ---")
    try:
        with open(config_json_path) as f:
            config_obj = json.load(f)
        print(json.dumps(config_obj, indent=2))

        # Save config.json into our timestamped subdir
        config_dst = os.path.join(out_dir, "config.json")
        with open(config_dst, "w") as out_f:
            json.dump(config_obj, out_f, indent=2)
        print(f"Saved config JSON to {config_dst}")
    except FileNotFoundError:
        print("config.json not found; run_skim_gpt may not have completed yet.")

    # 5b) Read, print, and save secrets.json
    secrets_json_path = os.path.join(job_dir, "secrets.json")
    print(f"\n--- Contents of {secrets_json_path} ---")
    try:
        with open(secrets_json_path) as f:
            secrets_obj = json.load(f)
        print(json.dumps(secrets_obj, indent=2))
        # Save secrets.json into our timestamped subdir
        secrets_dst = os.path.join(out_dir, "secrets.json")
        with open(secrets_dst, "w") as out_f:
            json.dump(secrets_obj, out_f, indent=2)
        print(f"Saved secrets JSON to {secrets_dst}")
    except FileNotFoundError:
        print("secrets.json not found; run_skim_gpt may not have completed yet.")

    # 6) Poll the GET endpoint until job finishes
    print("\nPolling for job completion…")
    while True:
        get_resp = requests.get(GET_URL, params={"id": job_id})
        get_resp.raise_for_status()
        status_json = get_resp.json()
        status = status_json.get("status")
        print("Status:", status)
        if status == "finished":
            print("Final result:")
            print(json.dumps(status_json.get("result"), indent=2))
            # Save final result JSON into our timestamped subdir
            result_dst = os.path.join(out_dir, "result.json")
            with open(result_dst, "w") as rf:
                json.dump(status_json.get("result"), rf, indent=2)
            print(f"Saved result JSON to {result_dst}")
            break
        if status in ("queued", "started"):
            time.sleep(2)
        else:
            print("Unexpected status:", status)
            break

if __name__ == "__main__":
    main() 