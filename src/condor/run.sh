# #!/bin/bash
echo "Running job on $(hostname)"
echo "GPUs assigned: $CUDA_VISIBLE_DEVICES"

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
