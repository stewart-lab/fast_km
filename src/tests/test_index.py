import os
from indexing.abstract_catalog import AbstractCatalog
from indexing.index import Index
from indexing.abstract import Abstract
from indexing.index_builder import IndexBuilder
import indexing.km_util as util

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

    cataloger.add_or_update_abstract(abs1, 'fake_file.gzip')
    cataloger.add_or_update_abstract(abs2, 'fake_file.gzip')
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

    query = the_index.query_index("the")
    assert query == set([abs1.pmid, abs2.pmid])

    query = the_index.query_index("are are are")
    assert query == set([abs2.pmid])

    query = the_index.query_index("are are are some")
    assert len(query) == 0

    query = the_index.query_index("are are are quick")
    assert len(query) == 0

    query = the_index.query_index("brown")
    assert query == set([abs1.pmid])

    assert the_index._publication_years[abs1.pmid] == abs1.pub_year
    assert the_index._publication_years[abs2.pmid] == abs2.pub_year

    query = the_index.query_index("test_test")
    assert len(query) == 0

    assert the_index.n_articles() == 2