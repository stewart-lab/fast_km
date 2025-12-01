import os
import pytest
from src.documents.document import Document
from src.indexing.index import Index
import src.global_vars as gvars
from src.jobs.index_corpus.job import run_indexing_job
from src.jobs.index_corpus.params import IndexingJobParams

@pytest.fixture
def temp_dir():
    return os.path.join(os.getcwd(), "tests", "tmp")

def test_indexing_job(temp_dir: str):
    # clean up any existing temp data
    _delete_temp_dir(temp_dir)
    _data_dir = str(gvars.data_dir)
    temp_data_dir = os.path.join(temp_dir, "index_job_data")
    gvars.data_dir = temp_data_dir

    # create a small corpus
    idx = Index(gvars.data_dir)
    doc1 = Document(pmid=1, pub_year=2020, title="test abstract", abstract="Test Text Version...1", origin="test1.xml.gz")
    idx.add_or_update_documents([doc1])
    idx.close()

    # run the indexing job
    params = IndexingJobParams()
    run_indexing_job(params)

    # verify the indexed documents
    idx = Index(gvars.data_dir)
    assert idx.count_documents() == 1
    assert idx.get_document(1).abstract == doc1.abstract
    idx.close()

    # clean up temp data
    _delete_temp_dir(temp_dir)
    gvars.data_dir = _data_dir

def _delete_temp_dir(temp_dir: str):
    if os.path.exists(temp_dir):
        for root, dirs, files in os.walk(temp_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(temp_dir)