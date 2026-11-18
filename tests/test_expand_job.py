import os
import shutil
import pytest
import src.global_vars as gvars
import src.indexing.synonyms as synonyms
import src.jobs.job_util as job_util
from src.documents.document import Document
from src.indexing.index import Index
from src.jobs.expand.job import run_expand_job
from src.jobs.expand.params import ExpandJobParams
from src.jobs.kinderminer.job import run_kinderminer_job
from src.jobs.kinderminer.params import KinderMinerJobParams
from src.fast_km_exception import FastKmException

@pytest.fixture
def data_dir():
    path = os.path.join(os.getcwd(), "tests", "tmp", "expand_job")
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    synonyms.save_synonym_table(path, 9999, [["Myocardial Infarction", "Heart Attack"]])
    synonyms._synonym_cache.clear()
    synonyms._last_checked.clear()
    synonyms.get_variants.cache_clear()

    idx = Index(path)
    idx.add_or_update_documents([
        Document(pmid=1, pub_year=2010, title="a study", abstract="a heart attack in a young man", origin="test.xml.gz"),
        Document(pmid=2, pub_year=2020, title="a study", abstract="myocardial infarction and aspirin", origin="test.xml.gz"),
        Document(pmid=3, pub_year=2020, title="a study", abstract="two heart attacks and aspirin", origin="test.xml.gz"),
    ])
    for _progress in idx.index_documents():
        pass
    idx.close()

    old_data_dir = gvars.data_dir
    gvars.data_dir = path
    yield path
    gvars.data_dir = old_data_dir
    shutil.rmtree(path, ignore_errors=True)

def test_expand_job(data_dir: str):
    [result] = run_expand_job(ExpandJobParams(terms=["heart attack"]))
    assert result["term"] == "heart attack"
    assert result["n_without_expansion"] == 1
    assert result["n_with_expansion"] == 3
    [subterm] = result["subterms"]
    assert subterm["subterm"] == "heart attack"
    counts = {v["variant"]: v["n"] for v in subterm["variants"]}
    assert counts["heart attack"] == 1
    assert counts["heart attacks"] == 1
    assert counts["myocardial infarction"] == 1

def test_expand_job_composite_terms_and_years(data_dir: str):
    [result] = run_expand_job(ExpandJobParams(terms=["heart attack|aspirin"], censor_year_lower=2015))
    assert [s["subterm"] for s in result["subterms"]] == ["heart attack", "aspirin"]
    assert result["n_with_expansion"] == 2  # PMID 1 is from 2010

def test_expand_job_without_counts(data_dir: str):
    [result] = run_expand_job(ExpandJobParams(terms=["heart attack"], counts=False))
    assert "n_with_expansion" not in result
    assert all(v["n"] is None for v in result["subterms"][0]["variants"])

def test_expand_job_does_not_cache_variants_on_disk(data_dir: str):
    run_expand_job(ExpandJobParams(terms=["acute heart attack"]))
    idx = Index(data_dir)
    cached = {row[0] for row in idx._idx_cursor.execute("SELECT term FROM query_cache")}
    idx.close()
    assert cached <= {"acute heart attack"}

def test_expand_job_limits_terms(data_dir: str):
    with pytest.raises(FastKmException):
        run_expand_job(ExpandJobParams(terms=["x"] * 201))

def test_kinderminer_job_records_expansions(data_dir: str, monkeypatch):
    class FakeJob:
        meta = {}
        def save_meta(self):
            pass
    job = FakeJob()
    monkeypatch.setattr(job_util, "get_current_job", lambda: job)

    run_kinderminer_job(KinderMinerJobParams(a_terms=["heart attack"], b_terms=["aspirin"], expand_synonyms=True))
    expansions = job.meta["expansions"]
    assert set(expansions) == {"heart attack", "aspirin"}
    assert ["heart attack", ["heart attack", "heart attacks", "myocardial infarction", "myocardial infarctions"]] in expansions["heart attack"]

    job.meta = {}
    run_kinderminer_job(KinderMinerJobParams(a_terms=["heart attack"], b_terms=["aspirin"]))
    assert "expansions" not in job.meta
