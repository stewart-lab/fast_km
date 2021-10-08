import os
import pytest
import gzip
import shutil
from ..src.index import Index
from ..src.abstract import Abstract
from ..src import index_abstracts as indexer
from ..src import km_util as util

@pytest.fixture
def data_dir():
    return os.path.join(os.getcwd(), "project", "tests", "test_data", "indexer")

def test_text_sanitization():
    text = "The qui-ck brown fox [jumped ]over the la'zy dog."
    sanitized = indexer.get_sanitized_text(text, r'[^\w\s]')
    assert sanitized == "The quick brown fox jumped over the lazy dog"

def test_ngrams():
    text = "The qui-ck brown fox [jumped ]over the la'zy dog."
    memory_buffer = []

    mono_grams = indexer.get_n_grams(text, 1, memory_buffer)
    assert "The" in mono_grams
    assert "quick" in mono_grams
    assert "lazy" in mono_grams
    assert "brown fox" not in mono_grams
    assert "brown fox jumped" not in mono_grams

    mono_and_bi_grams = indexer.get_n_grams(text, 2, memory_buffer)
    assert "The" in mono_and_bi_grams
    assert "brown fox" in mono_and_bi_grams
    assert "jumped over" in mono_and_bi_grams
    assert "lazy dog" in mono_and_bi_grams
    assert "brown fox jumped" not in mono_and_bi_grams

def test_get_files_to_index(data_dir):
    # pretend we have not indexed any files yet
    # should have 1 file remaining to index
    files_to_index = indexer.get_files_to_index(data_dir, [])
    assert len(files_to_index) == 1

    # pretend we've already indexed the pubmed21n1432.xml.gz file
    # should have 0 files remaining to index (no other .xml.gz files in dir)
    test_xml_file = "pubmed21n1432.xml.gz"
    files_to_index = indexer.get_files_to_index(data_dir, [test_xml_file])
    assert len(files_to_index) == 0

def test_parse_xml(data_dir):
    test_xml_file = os.path.join(data_dir, "pubmed21n1432.xml.gz")
    assert os.path.exists(test_xml_file)

    abstracts = []
    with gzip.open(test_xml_file, 'rb') as file:
        content = file.read()
        abstracts = indexer.parse_xml(content)

    assert len(abstracts) > 2800

    # test for proper italics tags removal
    abs_test = next(obj for obj in abstracts if obj.pmid == 34578158)
    assert abs_test.text.startswith("Aedes aegypti acts as a vector")

    # test for proper concatenation for abstracts with multiple sections
    abs_test = next(obj for obj in abstracts if obj.pmid == 34582133)
    assert abs_test.text.startswith("To describe healthcare professionals")
    assert abs_test.text.endswith("family members at the end-of-life.")

def test_indexer(data_dir):
    index_dir = indexer.get_index_dir(data_dir)

    # delete the index if it exists already
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    assert not os.path.exists(index_dir)

    # build the index
    index = indexer.index_abstracts(data_dir, 1, 1)

    # query the index
    query = index.query_index("polysaccharide")
    query = query | index.query_index("polysaccharides")
    query = query | index.query_index("lipopolysaccharides")
    query = query | index.query_index("lipopolysaccharide")
    query = query | index.query_index("exopolysaccharide")

    assert len(query) == 37

    # delete the index when the test is done
    shutil.rmtree(index_dir)