import os
import argparse
import subprocess
from dotenv import load_dotenv 
import uvicorn
from fastapi import FastAPI, HTTPException
import src.global_vars as gvars

# load environment variables
load_dotenv()
env_workers_high   = os.getenv('HIGH', None)
env_workers_medium = os.getenv('MEDIUM', None)
env_workers_low    = os.getenv('LOW', None)
env_redis          = os.getenv('REDIS', None)
env_postgres_host  = os.getenv('POSTGRES_HOST', '')
env_postgres_port  = os.getenv('POSTGRES_PORT', '')
env_postgres_user  = os.getenv('POSTGRES_USER', '')
env_postgres_pass  = os.getenv('POSTGRES_PASSWORD', '')
env_api_port       = os.getenv('API_PORT', None)
env_pubmed_key     = os.getenv('PUBMED_API_KEY', '')
env_openai_key     = os.getenv('OPENAI_API_KEY', '')
env_htcondor_token = os.getenv('HTCONDOR_TOKEN', '')
env_deepseek_key   = os.getenv('DEEPSEEK_API_KEY', '')
env_timezone       = os.getenv('TIMEZONE', '')

# parse command line args
parser = argparse.ArgumentParser()
parser.add_argument('--high',     type=int, default=1)
parser.add_argument('--medium',   type=int, default=1)
parser.add_argument('--low',      type=int, default=1)
parser.add_argument('--redis',    type=str, default='localhost:6379')
parser.add_argument('--api_port', type=int, default=8000)
parser.add_argument('--timezone', type=str, default='America/Chicago')
args = parser.parse_args()

# override command line args with environment variables if set
if env_workers_high:
    args.high = int(env_workers_high)
if env_workers_medium:
    args.medium = int(env_workers_medium)
if env_workers_low:
    args.low = int(env_workers_low)
if env_redis:
    args.redis = env_redis
if env_api_port:
    args.api_port = int(env_api_port)
if env_timezone:
    args.timezone = env_timezone

# set global variables
host, port = args.redis.split(':')
gvars.redis_host = host
gvars.redis_port = int(port)
gvars.fastapi_port = args.api_port
gvars.SECRET_PUBMED_API_KEY = env_pubmed_key
gvars.SECRET_OPENAI_API_KEY = env_openai_key
gvars.SECRET_HTCONDOR_TOKEN = env_htcondor_token
gvars.SECRET_DEEPSEEK_API_KEY = env_deepseek_key
gvars.timezone = args.timezone
gvars.POSTGRES_HOST = env_postgres_host
gvars.POSTGRES_PORT = int(env_postgres_port) if env_postgres_port else 0
gvars.POSTGRES_USER = env_postgres_user
gvars.POSTGRES_PASSWORD = env_postgres_pass

if not gvars.SECRET_PUBMED_API_KEY:
    print("WARNING: No PubMed API key set. Hypothesis evaluation jobs will fail.")
if not gvars.SECRET_OPENAI_API_KEY:
    print("WARNING: No OpenAI API key set. Hypothesis evaluation jobs will fail if using OpenAI models.")
if not gvars.SECRET_HTCONDOR_TOKEN:
    print("WARNING: No HTCondor token set. Hypothesis evaluation jobs will fail.")
if not gvars.SECRET_DEEPSEEK_API_KEY:
    print("WARNING: No DeepSeek API key set. Hypothesis evaluation jobs will fail if using DeepSeek models")

from src.jobs.serial_kinderminer.job import run_serial_kinderminer_job
from src.jobs.kinderminer.job import run_kinderminer_job
from src.jobs.hypothesis_eval.job import run_hypothesis_eval_job
from src.jobs.kinderminer.params import KinderMinerJobParams
from src.jobs.hypothesis_eval.params import HypothesisEvalJobParams
from src.jobs.index_corpus.params import IndexingJobParams
import src.documents.corpus_ops as crud
from src.jobs.workers import run_workers
from src.jobs.job_queue import queue_job, queue_indexing_job, get_job, cancel_job

app = FastAPI()
deprecated_tags = ["deprecated endpoints"]
km_tags = ["kinderminer"]
hyp_tags = ["hypothesis evaluation"]
index_tags = ["indexing"]
cancel_tags = ["cancel job"]
doc_tags = ["documents"]

@app.post("/api/kinderminer", tags=km_tags)
@app.post('/skim/api/jobs', tags=deprecated_tags)  # old endpoint for SKiM/KM
def submit_kinderminer_job(params: KinderMinerJobParams) -> dict:
    if params.c_terms:
        priority = 'HIGH' if len(params.a_terms) + len(params.b_terms) + len(params.c_terms) <= 50 else 'MEDIUM'
        return queue_job(run_serial_kinderminer_job, priority, params)
    else:
        priority = 'HIGH' if len(params.a_terms) + len(params.b_terms) <= 50 else 'MEDIUM'
        return queue_job(run_kinderminer_job, priority, params)

@app.post("/api/hypothesis_eval", tags=hyp_tags)
@app.post('/hypothesis_eval/api/jobs/', tags=deprecated_tags)  # old endpoint for hypothesis eval
def submit_hypothesis_eval_job(params: HypothesisEvalJobParams) -> dict:
    return queue_job(run_hypothesis_eval_job, 'LOW', params)

@app.post("/api/index", tags=index_tags)
def submit_index_job(params: IndexingJobParams) -> dict:
    return queue_indexing_job(params)

@app.post("/api/cancel_job", tags=cancel_tags)
@app.post('/cancel_job/api/jobs/', tags=deprecated_tags)  # old endpoint for cancel job
def cancel_job_by_id(id: str) -> dict:
    job_info = cancel_job(id)
    if not job_info:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_info

@app.get("/api/kinderminer", tags=km_tags)
@app.get("/api/hypothesis_eval", tags=hyp_tags)
@app.get("/api/index", tags=index_tags)
@app.get('/skim/api/jobs', tags=deprecated_tags)  # old endpoint for SKiM/KM
@app.get('/hypothesis_eval/api/jobs/', tags=deprecated_tags)  # old endpoint for hypothesis eval
def get_job_by_id(id: str) -> dict:
    job_info = get_job(id)
    if job_info is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_info

@app.post("/api/documents", tags=doc_tags)
def add_documents(params: crud.AddDocumentsParams) -> dict:
    return crud.add_or_update_corpus_docs(params)

@app.get("/api/documents", tags=doc_tags)
def get_documents(params: crud.GetDocumentsParams) -> dict:
    return crud.get_corpus_docs(params)

@app.get("/api/documents/origins", tags=doc_tags)
def get_document_origins() -> dict:
    return crud.get_corpus_doc_origins()

@app.delete("/api/documents", tags=doc_tags)
def delete_documents(params: crud.DeleteDocumentsParams) -> dict:
    return crud.delete_corpus_docs(params)

if __name__ == "__main__":
    # run job monitoring dashboard as a background process
    subprocess.Popen(["streamlit", "run", "src/dashboard.py", args.redis])

    # run workers as background processes
    run_workers(low=args.low, medium=args.medium, high=args.high, indexing=1)

    # run the fastapi server as the main process
    uvicorn.run(app, host="0.0.0.0", port=gvars.fastapi_port)