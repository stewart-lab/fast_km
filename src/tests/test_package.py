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
import shutil

# Base URL for the API. Use port 6002 to connect to the Docker container (mapped from container port 5000)
port = os.environ.get("FLASK_PORT", "6002")
API_BASE = f"http://localhost:{port}"
POST_URL = f"{API_BASE}/hypothesis_eval/api/jobs/"
GET_URL  = f"{API_BASE}/hypothesis_eval/api/jobs/"

# Sample payload matching the expected schema for a KM job
test_payload_km_single_row = {
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

test_payload_skim_single_row = {
    "model": "r1",
    "top_n_articles_most_cited": 7,
    "top_n_articles_most_recent": 7,
    "data": [
        {
            "a_term": "Alzheimer's",
            "b_term": "FYN",
            "c_term": "PP2",
            "ab_pmid_intersection": [
                9600986, 20655099, 21228179, 22820466, 20413653, 24012003, 11278378,
                9763511, 23576130, 14999081, 25394486, 12239221, 29843241, 16237174,
                23175838, 25492702, 11756483, 16762377, 19714494, 16014719, 26881137,
                15140940, 15853747, 23883936, 22090472, 23345405, 8388744, 24495408,
                25728499, 18096814, 11162638, 25093460, 28864542, 9952400, 25963821,
                16806510, 25874001, 27274066, 11165762, 20110615, 21794954, 15708437,
                20691893, 21692989, 28709498, 19294646, 25558816, 19542604, 27639648,
                24291102, 38086513, 37899058, 37565253, 37365538, 37149826, 37002649,
                36978877, 36639708, 36561735, 36258016, 35920260, 35820485, 35806088,
                35622130, 35563596, 35431249, 35327643, 35253743, 35177665, 34959172,
                34939537, 34864675, 34757788, 34480901, 34445804, 34432266, 34195199,
                34061007, 33466666, 33380425, 33363455, 33322202, 33175322, 33128789,
                32959649, 32925027, 32814048, 32751526, 32692785, 32665013, 32604886,
                32580508, 32467647, 32231518, 32096051, 32082134, 31951614, 31698056,
                31329216, 31312079
            ],
            "bc_pmid_intersection": [
                11278378, 11788577, 11684709, 17310986, 12431978, 10766791, 10366594,
                11445557, 11756483, 17363366, 10809781, 14761972, 16513647, 16014719,
                16540575, 12663749, 16891393, 19129465, 17974954, 11078745, 12515828,
                11546805, 18326860, 10945993, 15557120, 12007793, 11312300, 12846735,
                11170180, 19258392, 12529324, 21098033, 12921535, 10921917, 17030987,
                15831816, 21480388, 11483655, 15632127, 16267051, 18550847, 20431062,
                11452011, 16882656, 11056155, 16751367, 10858437, 16951152, 11468287,
                20844127, 36532777, 35185557, 34863113, 34326922, 34238111, 33883672,
                33153232, 32690822, 32579948, 30861161, 30735733, 30716585, 30419241,
                29937990, 29453318, 28993142, 28736554, 28535499, 28497343, 28343945,
                28266558, 27926612, 27624844, 26888964, 26449489, 25967238, 25377086,
                25354453, 25106729, 24787897, 24731946, 24428562, 23838580, 23805846,
                23510015, 23149288, 22957720, 22924694, 22447928, 22437915, 22354875,
                21940794, 21792912, 21697238, 21642769, 21592972, 21501596, 21056984
            ],
            "ac_pmid_intersection": [
                11278378, 11756483, 16014719, 11879813, 22892311, 12645527, 15748155,
                17267111, 23805846, 20441772, 27016018, 29712570, 28266558, 30735733,
                23733502, 7755608, 33536910, 34818135
            ]
        }
    ],
    "SKIM_hypotheses": {
        "BC": "There exists an interaction between the {c_term} and {b_term}",
        "AB": "There exists an interaction between the {a_term} and the gene {b_term}.",
        "rel_AC": "There exists an interaction between the {c_term} and the disease {a_term}.",
        "AC": "compound {c_term} reduces the chances of acquiring disease {a_term} or likely treats disease {a_term}.",
        "ABC": "compound {c_term} reduces the chances of acquiring disease {a_term} or likely treats disease {a_term} through its effect on gene {b_term}."
    }
}

test_payload_skim_multiple_rows = {
    "model": "o3-mini",
    "top_n_articles_most_cited": 7,
    "top_n_articles_most_recent": 8,
    "data": [
        {
            "a_term": "somitogenesis",
            "b_term": "FGF8",
            "c_term": "boundary",
            "ab_pmid_intersection": [
                9609821, 9671579, 15342488, 14697349, 15147762, 11091072, 14660549,
                16127714, 11044398, 14745965, 11677058, 15749084
            ],
            "bc_pmid_intersection": [
                9630220, 9609821, 11511349, 14527434, 9620855, 10021338, 9247335,
                12736208, 15691759, 11152636, 9671579, 10445503, 10821754, 10595509,
                15342488, 15105370, 10654611, 15347431, 11684653, 11748135, 10704829,
                10529429, 11500373, 12652306, 11493563, 11704761, 11731459, 10751174,
                11291867, 10357940, 11684654, 11861475, 12591239, 15147762, 11091072,
                12900448, 12843251, 10330495, 10375509, 10446345, 14660549, 15294862,
                12917294, 11923198, 15221377, 11960706, 12963112, 11036930, 15872005,
                11287195, 10457008, 14516697, 10433907, 10772799, 12617820, 11641225,
                11689282, 15590739, 9106162, 11133157, 15906236, 15254904, 15007826,
                11677058, 11520665, 10646796, 14757516, 11744364, 11803577, 9858666,
                11324311
            ],
            "ac_pmid_intersection": []
        },
        {
            "a_term": "somitogenesis",
            "b_term": "EPHA4",
            "c_term": "boundary",
            "ab_pmid_intersection": [
                9765210, 10330372, 13678588, 11562356, 11747084, 11973278, 11133162,
                11307170, 14516678, 12617855
            ],
            "bc_pmid_intersection": [
                9765210, 10887087, 10330372, 11182083, 11684655, 15797022, 13678588,
                10725246, 11562356, 11133162, 12794746, 11963655, 11686234, 11978390,
                14516678, 12617855
            ],
            "ac_pmid_intersection": []
        }
    ],
    "SKIM_hypotheses": {
        "BC": "There exists an interaction between the {c_term} and {b_term}",
        "AB": "There exists an interaction between the {a_term} and the gene {b_term}.",
        "rel_AC": "There exists an interaction between the {c_term} and the disease {a_term}.",
        "AC": "{c_term} treats {a_term}",
        "ABC": "{c_term} treats {a_term} through its effect on {b_term}"
    }
}

test_payload_km_multiple_rows = {
    "model": "o3-mini",
    "KM_hypothesis": "{a_term} informs {b_term}",
    "top_n_articles_most_cited": 7,
    "top_n_articles_most_recent": 8,
    "data": [
        {
            "a_term": "irritability",
            "b_term": "Antidepressants",
            "ab_pmid_intersection": [
                15697325,7317698,14994733,19703633,19701065,2195555,7779245,11869758,7608049,24828898,8498418,
                11332169,19478285,17288694,12892989,19373620,17513980,391342,11518474,9840194,21254788,21818629,
                26415692,17561352,15708416,16266753,18565592,26797170,24795586,10471170,21663425,15237244,17850879,
                16427176,12454558,12215058,23937313,22419332,18463341,24016840,15708425,19637072,3054623,21733477,
                19554671,19630716,6201941,11380644,22789402,17008624,22963063,9017765,19269045,18781664,23449756,
                16166193,11995774,16253231,24252377,15588754,15156244,16731214,28558366,18457344,22858216,20981764,
                8853610,16644768,9609678,12659404,25687279,11020543,16889102,9140631,26984349,22173265,16766043,
                9809215,12892015,8503747,22842021,17391823,26336379,31227264,25638794,15149296,7640830,22704403,
                27119382,2680239,15529462,25968482,26908089,8791021,9809220,23885348,21507130,26495770,11142177,
                25514063
            ]
        },
        {
            "a_term": "irritability",
            "b_term": "Antipsychotics",
            "ab_pmid_intersection": [
                14994733,15051107,18172517,15708417,18533764,25602248,21550212,15330685,10902094,15142390,19068333,
                25178749,21090835,22972123,23552907,26778658,22592735,23226952,27344135,15575418,23046144,27238064,
                2861801,24614763,20390357,22550944,21699271,24372896,22549762,28791693,18775367,23417276,16268664,
                20981764,9853693,21294670,21800056,25687279,16822549,30707602,29695153,31289093,21731831,17705564,
                16379518,27389348,26336379,15104522,24828014,14521196,21152171,30103719,25968482,22001766,15650505,
                20730109,24231168,17094930,16861101,23885348,28335658,24050741,26091194,22849533,22548111,17329303,
                30171515,11142177,24929957,27064142,31102885,24045605,25945321,24932108,26465194,26366961,26979176,
                23518064,27699845,27289228,22643087,33329124,22245026,29454305,29655072,29482036,30102079,25544354,
                30245571,28269767,26458342,26688788,32417789,26471516,24600266,24369879,24656684,26527556,29238235,
                28847182
            ]
        }
    ]
}


def main():
    # 1) Submit the job via POST
    print("Submitting POST to", POST_URL)
    response = requests.post(POST_URL, json=test_payload_skim_multiple_rows)
    response.raise_for_status()
    resp_json = response.json()
    job_id = resp_json.get("id")
    print("Received job ID:", job_id)

    # 2) Derive the temp job directory
    job_dir = f"/tmp/job-{job_id}"
    print("Expecting job files in:", job_dir)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_data_dir = os.path.abspath(os.path.join(script_dir, "test_results", "package_tests"))
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
    # Patterns for streaming log files while the job is running
    log_patterns = [
        "run_*.log",                    # wildcard HTCondor logs in root
        "output/std_out.out",           # stdout in output subdir
        "output/std_err.err",           # stderr in output subdir
        "std_out.out",                  # stdout in root
        "std_err.err"                   # stderr in root
    ]

    def copy_logs():
        """Copy any new log/.out/.err files from job_dir to our test output directory."""
        import glob
        for pattern in log_patterns:
            if "*" in pattern:
                candidates = glob.glob(os.path.join(job_dir, pattern))
            else:
                path = os.path.join(job_dir, pattern)
                candidates = [path] if os.path.exists(path) else []
            for src in candidates:
                if os.path.exists(src):
                    dst = os.path.join(out_dir, os.path.basename(src))
                    try:
                        shutil.copy2(src, dst)
                        print(f"Streamed {os.path.basename(src)} to {dst}")
                    except Exception as e:
                        print(f"Warning: could not stream {src}: {e}")

    while True:
        get_resp = requests.get(GET_URL, params={"id": job_id})
        get_resp.raise_for_status()
        status_json = get_resp.json()
        status = status_json.get("status")
        print("Status:", status)
        if status == "finished":
            print("Final result:")
            result_data = status_json.get("result")
            print(json.dumps(result_data, indent=2))
            
            # Save final result JSON into our timestamped subdir
            result_dst = os.path.join(out_dir, "result.json")
            with open(result_dst, "w") as rf:
                json.dump(result_data, rf, indent=2)
            print(f"Saved result JSON to {result_dst}")
            
            # If result is a dictionary with multiple results, save each one separately  
            if isinstance(result_data, dict):
                for key, value in result_data.items():
                    individual_result_dst = os.path.join(out_dir, f"result_{key}.json")
                    with open(individual_result_dst, "w") as rf:
                        json.dump(value, rf, indent=2)
                    print(f"Saved individual result to result_{key}.json")
                    
                print(f"Parsed {len(result_data)} individual results from the job")
            # Final flush of logs before exiting polling loop
            copy_logs()
            break
        if status in ("queued", "started"):
            # Stream logs periodically before next poll
            copy_logs()
            time.sleep(2)
        else:
            print("Unexpected status:", status)
            break

    # 7) Copy HTCondor log files to test directory for persistence
    print(f"\n--- Copying HTCondor Log Files ---")
    # Check for log files in multiple locations
    log_patterns = [
        "run_*.log",                    # HTCondor job logs in root
        "output/std_out.out",           # Standard output in output subdirectory
        "output/std_err.err",           # Standard error in output subdirectory
        "std_out.out",                  # Also check root directory
        "std_err.err"                   # Also check root directory
    ]
    copied_logs = []
    
    for pattern in log_patterns:
        import glob
        if "*" in pattern:
            # Use glob for wildcard patterns
            log_files = glob.glob(os.path.join(job_dir, pattern))
        else:
            # Direct path check for specific files
            log_path = os.path.join(job_dir, pattern)
            log_files = [log_path] if os.path.exists(log_path) else []
            
        for log_path in log_files:
            if os.path.exists(log_path):
                log_filename = os.path.basename(log_path)
                log_dst = os.path.join(out_dir, log_filename)
                try:
                    shutil.copy2(log_path, log_dst)
                    copied_logs.append(log_filename)
                    print(f"✓ Copied {log_filename} to test directory")
                    
                    # Show a preview of the log content
                    try:
                        with open(log_path, 'r') as f:
                            content = f.read()
                            lines = content.split('\n')
                            if len(lines) > 10:
                                preview = '\n'.join(lines[:5] + ['...'] + lines[-5:])
                            else:
                                preview = content
                            print(f"  Preview of {log_filename}:")
                            print(f"  {preview[:200]}{'...' if len(preview) > 200 else ''}")
                    except Exception as e:
                        print(f"  Could not preview {log_filename}: {e}")
                        
                except Exception as e:
                    print(f"✗ Failed to copy {log_filename}: {e}")
    
    if copied_logs:
        print(f"Successfully copied {len(copied_logs)} log file(s): {', '.join(copied_logs)}")
    else:
        print("No HTCondor log files found to copy")
    
    print(f"\n✅ Test completed! All files saved to: {out_dir}")
    print(f"Files in test directory:")
    for file in sorted(os.listdir(out_dir)):
        file_path = os.path.join(out_dir, file)
        size = os.path.getsize(file_path)
        print(f"  {file} ({size} bytes)")

if __name__ == "__main__":
    main() 