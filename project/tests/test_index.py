import os
from ..src.index import Index

def test_index(tmp_path):
    db_path = os.path.join(tmp_path, 'db.db')
    exists = os.path.exists(db_path)
    assert not exists

    # create the index and store some data
    pmid1 = 1
    pmid2 = 2
    pmid3 = 4
    pub_year1 = 2020
    pub_year2 = 1984
    pub_year3 = 2019

    the_index = Index(db_path)
    the_index.start_building_index()
    the_index.place_value("test", pmid1, pub_year1)
    the_index.place_value("test", pmid2, pub_year2)
    the_index.place_value("test2", pmid3, pub_year3)

    # write the index to disk
    the_index.dump_cache_to_db()
    the_index.finish_building_index()

    # load the index from disk
    loaded_index = Index(db_path)
    query = loaded_index.query_index("test")
    
    # check that index contains expected contents and can be queried
    expected_set = set([pmid1, pmid2])
    assert len(expected_set & query) == 2
    assert len(expected_set ^ query) == 0
    assert loaded_index.get_publication_year(pmid1) == pub_year1
    assert loaded_index.get_publication_year(pmid2) == pub_year2