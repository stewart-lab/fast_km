import os
from indexing.abstract_catalog import AbstractCatalog
from indexing.index import Index
from indexing.abstract import Abstract
from indexing.index_builder import IndexBuilder
import indexing.km_util as util
import workers.loaded_index as li
from workers.work import skim_work
import json

def test_index_abstract(tmp_path):
    assert not os.path.exists(util.get_index_dir(tmp_path))

    cataloger = AbstractCatalog(tmp_path)
    abs1 = Abstract(1000, 2020, "A Really Cool Pubmed Abstract",
        "The quick brown fox jumped over the lazy dog.")

    assert abs1.pmid == 1000
    assert abs1.pub_year == 2020
    assert abs1.title == "A Really Cool Pubmed Abstract"
    assert abs1.text == "The quick brown fox jumped over the lazy dog."

    abs2 = Abstract(1001, 2021, "A Cool Title",
        "Some of the words are are repeated but some are-are-are not.")

    ct = {1000:1, 1001:2}
    
    with open(str(tmp_path) + "/test.json", "w", encoding="utf-8") as f:
            json.dump(ct, f)
    cataloger.add_or_update_abstract(abs1)
    cataloger.add_or_update_abstract(abs2)
    cataloger.write_catalog_to_disk(util.get_abstract_catalog(tmp_path))

    indexer = IndexBuilder(tmp_path)
    hot_storage = dict()
    cold_storage = dict()
    indexer._index_abstract(abs1, hot_storage)
    indexer._serialize_hot_to_cold_storage(hot_storage, cold_storage)
    indexer._write_index_to_disk(cold_storage)

    indexer._index_abstract(abs2, hot_storage)
    indexer._serialize_hot_to_cold_storage(hot_storage, cold_storage, True)
    indexer._write_index_to_disk(cold_storage)

    the_index = Index(tmp_path)
    the_index._init_pub_years()

    query = the_index._query_index("the")
    assert query == set([abs1.pmid, abs2.pmid])

    query = the_index._query_index("are are are")
    assert query == set([abs2.pmid])

    query = the_index._query_index("are are are some")
    assert len(query) == 0

    query = the_index._query_index("are are are quick")
    assert len(query) == 0

    query = the_index._query_index("brown")
    assert query == set([abs1.pmid])

    assert the_index._publication_years[abs1.pmid] == abs1.pub_year
    assert the_index._publication_years[abs2.pmid] == abs2.pub_year

    query = the_index._query_index("test_test")
    assert len(query) == 0

    assert the_index.n_articles() == 2

def test_citation_count(tmp_path):
    assert not os.path.exists(util.get_index_dir(tmp_path))

    cataloger = AbstractCatalog(tmp_path)
    abs1 = Abstract(1000, 2020, "A Really Cool Pubmed Abstract",
        "test The quick brown fox jumped over the lazy dog repeated the repeat.")
    abs2 = Abstract(1001, 2021, "A Cool Title",
        "Some of the words are are the repeated repeat but some are-are-are not.")
    abs3 = Abstract(1002, 2022, "sdfsb  rgtd gfhdfg",
        "Test repeat test repeat")
    ct = {"1000":"1", "1001":"2", "1002":"3"}
    
    cataloger.add_or_update_abstract(abs1)
    cataloger.add_or_update_abstract(abs2)
    cataloger.add_or_update_abstract(abs3)
    cataloger.write_catalog_to_disk(util.get_abstract_catalog(tmp_path))

    with open(util.get_icite_file(tmp_path), "w", encoding="utf-8") as f:
        json.dump(ct, f)
        
    indexer = IndexBuilder(tmp_path)
    hot_storage = dict()
    cold_storage = dict()
    indexer._index_abstract(abs1, hot_storage)
    indexer._serialize_hot_to_cold_storage(hot_storage, cold_storage)
    indexer._write_index_to_disk(cold_storage)

    indexer._index_abstract(abs2, hot_storage)
    indexer._serialize_hot_to_cold_storage(hot_storage, cold_storage, True)
    indexer._write_index_to_disk(cold_storage)

    indexer._index_abstract(abs3, hot_storage)
    indexer._serialize_hot_to_cold_storage(hot_storage, cold_storage, True)
    indexer._write_index_to_disk(cold_storage)

    the_index = Index(tmp_path)
    li.the_index = the_index

    query = {
        "a_terms": ["test"],
        "b_terms": ["repeat"],
        "c_terms": ["the"],
        "top_n": 50,
        "ab_fet_threshold": 1.0,
        "return_pmids": 'True'
    }
    val = skim_work(query)
    assert val[0]['bc_pmid_intersection'] == "{1001: '2', 1000: '1'}"
    print(val)