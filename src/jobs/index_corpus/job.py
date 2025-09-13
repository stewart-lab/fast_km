from src.indexing.index import Index
import src.global_vars as gvars
from src.jobs.index_corpus.params import IndexingJobParams, validate_params
from src.jobs.job_util import report_progress

def run_indexing_job(params: IndexingJobParams):
    validate_params(params)

    idx = Index(gvars.data_dir)

    for progress in idx.index_documents():
        report_progress(progress)

    idx.close()