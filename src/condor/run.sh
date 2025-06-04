# #!/bin/bash
echo "Running job on $(hostname)"
echo "GPUs assigned: $CUDA_VISIBLE_DEVICES"

# Debugging: Print all arguments received by run.sh
# echo "Number of arguments received: $#"
# echo "Arguments received: $*"
# echo "First argument (\$1): '$1'"
# echo "Second argument (\$2): '$2'" # Just in case something unexpected is happening

# echo "--- Network Connectivity Test (using curl) ---"
date

# # Function to test URL and check HTTP status
# test_url() {
#   URL="$1"
#   NAME="$2"
#   echo "--- Testing ${NAME} (URL: ${URL}) ---"
#   echo "--- Raw curl -Is output for ${NAME}: ---"
#   curl -Is --connect-timeout 10 --max-time 30 "${URL}" 2>&1 # Redirect stderr to stdout to capture all output
#   echo "--- End raw curl output for ${NAME} ---"
# }

# # Test Google (basic internet)
# test_url "http://google.com" "Google (HTTP)"

# # Test PubMed E-utilities (HTTPS)
# test_url "https://eutils.ncbi.nlm.nih.gov" "PubMed E-utilities (HTTPS)"

# # Test Hugging Face (HTTPS)
# test_url "https://huggingface.co" "Hugging Face (HTTPS)"

# echo "--- End Network Connectivity Test ---"


# Debugging: Try printing standard HTCondor environment variables
echo "--- HTCondor Environment Variables ---"
echo "_CONDOR_ITEM (env var): $_CONDOR_ITEM" # Common env var for Item
echo "_CONDOR_JOBID (env var): $_CONDOR_JOBID"
echo "_CONDOR_CLUSTERID (env var): $_CONDOR_CLUSTERID"
echo "_CONDOR_PROCID (env var): $_CONDOR_PROCID"
echo "_CONDOR_JOBAD_RAW (env var): $_CONDOR_JOBAD_RAW" # Raw JobAd (might be large)
echo "--- End of HTCondor Environment Variables ---"

# Define file paths (fixed - these were undefined before!)
files_txt="files.txt"
config_json="config.json"
secrets_json="secrets.json"

export TRANSFORMERS_CACHE=$_CONDOR_SCRATCH_DIR/models
export HF_HOME=$_CONDOR_SCRATCH_DIR/models
export HF_DATASETS_CACHE=$_CONDOR_SCRATCH_DIR/datasets
export HF_MODULES_CACHE=$_CONDOR_SCRATCH_DIR/modules
export HF_METRICS_CACHE=$_CONDOR_SCRATCH_DIR/metrics

# Set GPU mode for skimgpt package
export SKIMGPT_GPU_MODE=true

skimgpt-relevance --km_output "$files_txt" --config "$config_json" --secrets "$secrets_json"
