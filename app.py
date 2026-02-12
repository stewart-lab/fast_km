import os
import argparse
import subprocess
import datetime
from dotenv import load_dotenv
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import src.global_vars as gvars
from fastapi.responses import JSONResponse

# load environment variables
load_dotenv()
env_workers_high   = os.getenv('HIGH', None)
env_workers_medium = os.getenv('MEDIUM', None)
env_workers_low    = os.getenv('LOW', None)
env_redis          = os.getenv('REDIS', None)
env_api_port       = os.getenv('API_PORT', None)
env_pubmed_key     = os.getenv('PUBMED_API_KEY', '')
env_openai_key     = os.getenv('OPENAI_API_KEY', '')
env_htcondor_token = os.getenv('HTCONDOR_TOKEN', '')
env_deepseek_key   = os.getenv('DEEPSEEK_API_KEY', '')
env_timezone       = os.getenv('TIMEZONE', '')
env_password       = os.getenv('PASSWORD', '')

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
from src.knowledge_graph.params import AddRelationshipsParams, GetRelationshipsParams
import src.documents.corpus_ops as corpus
from src.knowledge_graph.knowledge_graph import KnowledgeGraph
from src.auth.functions import authenticate
from src.jobs.workers import run_workers
from src.jobs.job_queue import queue_job, queue_indexing_job, get_job, cancel_job
from src.populate_db import populate_db
from src.populate_kg import populate_kg


@asynccontextmanager
async def lifespan(app: FastAPI):

    scheduler = AsyncIOScheduler()
    scheduler.add_job(populate_db, "cron", day_of_week="sat", hour=5, minute=0)
    # run populate_kg once, shortly after the app starts serving requests
    run_time = datetime.datetime.now() + datetime.timedelta(seconds=5)
    scheduler.add_job(populate_kg, "date", run_date=run_time)
    scheduler.start()

    yield


app = FastAPI(lifespan=lifespan)
deprecated_tags = ["deprecated endpoints"]
km_tags = ["kinderminer"]
hyp_tags = ["hypothesis evaluation"]
index_tags = ["indexing"]
cancel_tags = ["cancel job"]
doc_tags = ["documents"]
kg_tags = ["knowledge graph"]

security = HTTPBasic(auto_error=False)

def verify_password(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials is None:
        try_pass = ''
    else:
        try_pass = credentials.password

    if not authenticate(try_pass, env_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed; password is incorrect.",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


@app.get("/health")
def health_check() -> dict:
    return {"status": "healthy"}


### Job management endpoints
@app.post("/api/kinderminer", tags=km_tags, status_code=status.HTTP_202_ACCEPTED)
@app.post('/skim/api/jobs', tags=deprecated_tags, status_code=status.HTTP_202_ACCEPTED)  # old endpoint for SKiM/KM
def submit_kinderminer_job(params: KinderMinerJobParams, authorized: bool = Depends(verify_password)) -> JSONResponse:
    if params.c_terms:
        priority = 'HIGH' if len(params.a_terms) + len(params.b_terms) + len(params.c_terms) <= 50 else 'MEDIUM'
        result = queue_job(run_serial_kinderminer_job, priority, params)
    else:
        priority = 'HIGH' if len(params.a_terms) + len(params.b_terms) <= 50 else 'MEDIUM'
        result = queue_job(run_kinderminer_job, priority, params)
    return JSONResponse(content=result, status_code=202)

@app.post("/api/hypothesis_eval", tags=hyp_tags, status_code=status.HTTP_202_ACCEPTED)
@app.post('/hypothesis_eval/api/jobs/', tags=deprecated_tags, status_code=status.HTTP_202_ACCEPTED)  # old endpoint for hypothesis eval
def submit_hypothesis_eval_job(params: HypothesisEvalJobParams, authorized: bool = Depends(verify_password)) -> JSONResponse:
    result = queue_job(run_hypothesis_eval_job, 'LOW', params)
    return JSONResponse(content=result, status_code=202)

@app.post("/api/index", tags=index_tags, status_code=status.HTTP_202_ACCEPTED)
def submit_index_job(params: IndexingJobParams, authorized: bool = Depends(verify_password)) -> JSONResponse:
    result = queue_indexing_job(params)
    return JSONResponse(content=result, status_code=202)

@app.post("/api/cancel_job", tags=cancel_tags)
@app.post('/cancel_job/api/jobs/', tags=deprecated_tags)  # old endpoint for cancel job
def cancel_job_by_id(id: str, authorized: bool = Depends(verify_password)) -> dict:
    job_info = cancel_job(id)
    if not job_info:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_info

@app.get("/api/kinderminer", tags=km_tags)
@app.get("/api/hypothesis_eval", tags=hyp_tags)
@app.get("/api/index", tags=index_tags)
@app.get('/skim/api/jobs', tags=deprecated_tags)  # old endpoint for SKiM/KM
@app.get('/hypothesis_eval/api/jobs/', tags=deprecated_tags)  # old endpoint for hypothesis eval
def get_job_by_id(id: str, authorized: bool = Depends(verify_password)) -> dict:
    job_info = get_job(id)
    if job_info is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_info

### Document management endpoints
@app.post("/api/documents", tags=doc_tags)
def add_documents(params: corpus.AddDocumentsParams, authorized: bool = Depends(verify_password)) -> dict:
    return corpus.add_or_update_corpus_docs(params)

@app.get("/api/documents", tags=doc_tags)
def get_documents(params: corpus.GetDocumentsParams, authorized: bool = Depends(verify_password)) -> dict:
    return corpus.get_corpus_docs(params)

@app.get("/api/documents/origins", tags=doc_tags)
def get_document_origins(authorized: bool = Depends(verify_password)) -> dict:
    return corpus.get_corpus_doc_origins()

@app.delete("/api/documents", tags=doc_tags)
def delete_documents(params: corpus.DeleteDocumentsParams, authorized: bool = Depends(verify_password)) -> dict:
    return corpus.delete_corpus_docs(params)

### Knowledge graph endpoints
@app.get("/api/knowledge_graph", tags=kg_tags)
def get_relationships(params: GetRelationshipsParams, authorized: bool = Depends(verify_password)) -> dict:
    kg = KnowledgeGraph(data_dir=gvars.data_dir)
    relationships = kg.get_relationships(params.entity1, params.entity2)
    kg.close()
    return {"status": "finished", "result": relationships}

@app.post("/api/knowledge_graph", tags=kg_tags)
def add_relationships(params: AddRelationshipsParams, authorized: bool = Depends(verify_password)) -> dict:
    kg = KnowledgeGraph(data_dir=gvars.data_dir)
    kg.add_relationships(params.relationships)
    kg.close()
    return {"status": "finished", "result": f"Added {len(params.relationships)} relationships to the knowledge graph."}

if __name__ == "__main__":
    # run job monitoring dashboard as a background process
    subprocess.Popen(["streamlit", "run", "src/dashboard.py", args.redis])

    # run workers as background processes
    run_workers(low=args.low, medium=args.medium, high=args.high, indexing=1)

    # run the fastapi server as the main process
    uvicorn.run(app, host="0.0.0.0", port=gvars.fastapi_port)