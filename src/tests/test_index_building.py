import os
import pytest
import gzip
import shutil
import pickle
from indexing.abstract_catalog import _parse_xml
from indexing.index import Index
from indexing.abstract import Abstract
from indexing.index_builder import IndexBuilder
from indexing.abstract_catalog import AbstractCatalog
from indexing import km_util as util

@pytest.fixture
def data_dir():
    return os.path.join(os.getcwd(), "src", "tests", "test_data", "indexer")

def delete_existing_index(data_dir):
    index_dir = util.get_index_dir(data_dir)

    # delete the index if it exists already
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    assert not os.path.exists(index_dir)

def test_tokenization():
    text = "The quick brown fox jumped over the lazy dog."

    tokens = util.get_tokens(text)
    assert "the" in tokens
    assert "quick" in tokens
    assert "lazy" in tokens
    assert "brown fox" not in tokens
    assert "brown fox jumped" not in tokens

def test_get_files_to_index(data_dir):
    delete_existing_index(data_dir)

    # pretend we have not indexed any files yet
    # should have 1 file remaining to index
    indexer = AbstractCatalog(data_dir)
    files_to_index = indexer._get_files_to_catalog()
    assert len(files_to_index) == 1

    # pretend we've already indexed the pubmed21n1432.xml.gz file
    # should have 0 files remaining to index (no other .xml.gz files in dir)
    test_xml_file = "pubmed21n1432.xml.gz"
    indexer.abstract_files.append(test_xml_file)
    files_to_index = indexer._get_files_to_catalog()
    assert len(files_to_index) == 0

def test_parse_xml(data_dir):
    delete_existing_index(data_dir)
    
    test_xml_file = os.path.join(data_dir, "pubmed21n1432.xml.gz")
    assert os.path.exists(test_xml_file)

    with gzip.open(test_xml_file, 'rb') as file:
        content = file.read()
        abstracts = _parse_xml(content)
    assert len(abstracts) > 2800

    # test for proper italics tags removal
    abs_test = next(obj for obj in abstracts if obj.pmid == 34578158)
    assert abs_test.text.startswith("Aedes aegypti acts as a vector")

    # test for proper concatenation for abstracts with multiple sections
    abs_test = next(obj for obj in abstracts if obj.pmid == 34582133)
    assert abs_test.text.startswith("To describe healthcare professionals")
    assert abs_test.text.endswith("family members at the end-of-life.")

def test_indexer(data_dir):
    delete_existing_index(data_dir)

    # build the index
    indexer = IndexBuilder(data_dir)
    indexer.build_index()
    
    # query the index
    index = Index(data_dir)
    query = index._query_index("polysaccharide")
    query = query | index._query_index("polysaccharides")
    query = query | index._query_index("lipopolysaccharides")
    query = query | index._query_index("lipopolysaccharide")
    query = query | index._query_index("exopolysaccharide")

    assert len(query) == 37

def test_abstract_cataloging(tmp_path):
    cataloger = AbstractCatalog(tmp_path)

    abs = Abstract(1000, 1993, "This is a cool title", "Interesting text")
    abs2 = Abstract(1001, 1994, "An interesting title", "Cool text")
    cataloger.add_or_update_abstract(abs)
    cataloger.add_or_update_abstract(abs2)

    path = os.path.join(tmp_path, 'abstract_catalog.txt.gzip')
    cataloger.write_catalog_to_disk(path)

    cataloger2 = AbstractCatalog(tmp_path)
    abstracts = []
    for item in cataloger2.stream_existing_catalog(path):
        abstracts.append(item)

    assert len(abstracts) == 2
    assert abstracts[0].title == abs.title
    assert abstracts[1].title == abs2.title

def test_abstract_cataloging_real_file(data_dir):
    delete_existing_index(data_dir)

    path = util.get_abstract_catalog(data_dir)

    cataloger = AbstractCatalog(data_dir)
    cataloger.catalog_abstracts()

    # TODO: assertions
    i = 0
    for abs in cataloger.stream_existing_catalog(path):
        i += 1
        assert abs.pmid > 0

    assert i == 4139

    # this abstract has "Dimocarpus longan" in italics in the title.
    # this code makes sure it's parsed correctly.
    pmid = 34577997

    cataloger = AbstractCatalog(data_dir)
    cataloger.catalog_abstracts()

    article = pickle.loads(cataloger.catalog[pmid])
    title = article.title

    assert 'Dimocarpus longan' in title
    assert 'Peel Extract as Bio-Based' in title
    