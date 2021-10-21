import os
from ..indexing.index import Index
from ..indexing.abstract import Abstract

def test_index_abstract(tmp_path):
    db_path = os.path.join(tmp_path, 'db.db')
    exists = os.path.exists(db_path)
    assert not exists

    the_index = Index(db_path)
    abs1 = Abstract(1000, 2020, "A Really Cool Pubmed Abstract",
        "The quick brown fox jumped over the lazy dog.")

    assert abs1.pmid == 1000
    assert abs1.pub_year == 2020
    assert abs1.title == "A Really Cool Pubmed Abstract"
    assert abs1.text == "The quick brown fox jumped over the lazy dog."

    abs2 = Abstract(1001, 2021, "A Cool Title",
        "Some of the words are are repeated but some are-are-are not.")

    the_index.index_abstract(abs1)
    the_index.dump_index_to_trie()
    the_index.index_abstract(abs2)
    the_index.finish_building_index()
    the_index = Index(db_path)

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

    assert the_index.get_publication_year(abs1.pmid) == abs1.pub_year
    assert the_index.get_publication_year(abs2.pmid) == abs2.pub_year

    query = the_index.query_index("test_test")
    assert len(query) == 0

    assert the_index.n_articles() == 2