import os
import pytest
import src.indexing.indexing_util as util
from src.documents.document import Document
from src.indexing.index import Index

@pytest.fixture
def temp_dir():
    return os.path.join("/scratch", "tests", "tmp")

def test_tokenization():
    text = "The_quick brown fox jumped over the lazy dog."

    tokens = util._tokenize_text(text, keep_logical_operators=False)
    assert "the" in tokens
    assert "quick" in tokens
    assert "lazy" in tokens
    assert "brown fox" not in tokens
    assert "brown fox jumped" not in tokens

def test_sanitize_term():
    term = "my_search & term"
    sanitized = util.sanitize_term_for_search(term, keep_logical_operators=True)
    assert sanitized == "my search&term"

    sanitized_no_ops = util.sanitize_term_for_search(term, keep_logical_operators=False)
    assert sanitized_no_ops == "my search term"

def test_get_subterms():
    composite_term = "term1 & term2 | term3"
    subterms = util.get_subterms(composite_term, keep_logical_operators=True)
    assert subterms == ["term1", "&", "term2", "|", "term3"]

    subterms_no_ops = util.get_subterms(composite_term, keep_logical_operators=False)
    assert subterms_no_ops == ["term1", "term2", "term3"]

def test_get_ngrams():
    term = "the quick brown fox"

    uni_grams = util.get_ngrams(term, n=[1])
    assert "the" in uni_grams
    assert "quick" in uni_grams
    assert "the quick" not in uni_grams
    assert "brown fox" not in uni_grams
    assert "the quick brown" not in uni_grams

    bi_grams = util.get_ngrams(term, n=[2])
    assert "the quick" in bi_grams
    assert "quick brown" in bi_grams
    assert "brown fox" in bi_grams
    assert "the" not in bi_grams
    assert "fox" not in bi_grams

    uni_bi_ngrams = util.get_ngrams(term, n=[1, 2])
    assert "the" in uni_bi_ngrams
    assert "the quick" in uni_bi_ngrams
    assert "fox" in uni_bi_ngrams
    assert "quick brown" in uni_bi_ngrams
    assert "the quick brown" not in uni_bi_ngrams

def test_index_abstract():
    index_update = dict()
    abstract = Document(pmid=12345, pub_year=2020, title="The quick brown fox", abstract="jumps over the lazy dog", origin="test.xml.gz")
    util.index_document(abstract, index_update)

    # Check some unigrams
    assert "the" in index_update
    assert "quick" in index_update

    # Check some bigrams
    assert "the quick" in index_update
    assert "brown fox" in index_update

    # Check PMIDs and positions
    assert 12345 in index_update["the"]
    assert 12345 in index_update["the quick"]
    assert len(index_update["brown fox"][12345]) == 1  # "brown fox" appears once
    assert index_update["brown fox"][12345][0] == 2  # position of "brown fox" in title

def test_indexing(temp_dir: str):
    # clean up any existing temp data
    _delete_temp_dir(temp_dir)

    temp_data_dir = os.path.join(temp_dir, "indexing")
    idx = Index(temp_data_dir)
    assert idx is not None
    assert idx.data_dir == temp_data_dir

    # add a document with multiple versions
    doc_v1 = Document(pmid=1, pub_year=2020, title="test abstract", abstract="Test Text Version...1", origin="test1.xml.gz")
    doc_v2 = Document(pmid=1, pub_year=2020, title="test abstract", abstract="Test Text Version...2", origin="test1.xml.gz")
    idx.add_or_update_documents([doc_v1, doc_v2])

    # update citation count
    citation_update_doc_1 = Document(pmid=1, pub_year=None, title=None, abstract=None, origin=None, citation_count=5)
    citation_update_doc_2 = Document(pmid=2, pub_year=None, title=None, abstract=None, origin=None, citation_count=10) # should be ignored, PMID 2 is not in corpus
    idx.add_or_update_documents([citation_update_doc_1, citation_update_doc_2])

    # index the documents
    for _progress in idx.index_documents():
        pass

    # check that only the latest version is indexed
    assert idx.count_documents() == 1
    assert idx.count_documents(2020, 2020) == 1
    assert idx.count_documents(2019, 2019) == 0
    assert idx.count_documents(2019, 2021) == 1
    assert idx.count_documents(2021, 2022) == 0
    assert idx.get_document(1).abstract == doc_v2.abstract
    assert idx.get_document(1).body is not None
    assert idx.get_document(1).citation_count == 5
    assert idx.get_document(2) is None
    assert idx.search_documents("version 1") == set()
    assert idx.search_documents("version 2") == {1}
    assert idx.search_documents("nonexistent") == set()
    assert idx.is_indexing_in_progress() == False

    # note that we have to clear the cache before each call here. this is sort of unique
    # to the testing environment where we want to test multiple year ranges.
    # in a "real life" querying scenario the user would only call search_documents
    # with one year range per job.
    idx.delete_term_from_memory("version 2")
    assert idx.search_documents("version 2", start_year=2020, end_year=2020) == {1}
    idx.delete_term_from_memory("version 2")
    assert idx.search_documents("version 2", start_year=2019, end_year=2019) == set()
    idx.delete_term_from_memory("version 2")
    assert idx.search_documents("version 2", start_year=2019, end_year=2021) == {1}
    idx.delete_term_from_memory("version 2")
    assert idx.search_documents("version 2", start_year=2021, end_year=2022) == set()
    idx.delete_term_from_memory("version 2")

    # close and reopen the index to make sure we can reload the index properly
    idx.close()
    idx = Index(temp_data_dir)
    assert idx.count_documents() == 1
    assert idx.get_document(1).abstract == doc_v2.abstract
    assert idx.get_document(1).citation_count == 5
    assert idx.search_documents("version 1") == set()
    assert idx.search_documents("version 2") == {1}

    # update the index with new versions of the same document
    doc_v3 = Document(pmid=1, pub_year=2020, title="test abstract", abstract="Test Text Version...3", origin="test2.xml.gz")
    doc_v4 = Document(pmid=1, pub_year=2020, title="test abstract", abstract="Test Text Version...4", origin="test2.xml.gz")
    idx.add_or_update_documents([doc_v3, doc_v4])
    for _progress in idx.index_documents():
        pass

    # check that only the latest version is indexed
    assert idx.count_documents() == 1
    assert idx.get_document(1).abstract == doc_v4.abstract
    assert idx.search_documents("version 1") == set()
    assert idx.search_documents("version 2") == set()
    assert idx.search_documents("version 3") == set()
    assert idx.search_documents("version 4") == {1}
    assert idx.search_documents("nonexistent") == set()

    idx.close()

    # clean up temp data
    _delete_temp_dir(temp_dir)


def _delete_temp_dir(temp_dir: str):
    if os.path.exists(temp_dir):
        for root, dirs, files in os.walk(temp_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(temp_dir)