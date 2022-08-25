import pytest
import os
import shutil
from indexing.index import Index
from indexing.index_builder import IndexBuilder
from workers import kinderminer as km
from indexing import km_util as util
from .test_index_building import data_dir
from workers import work
import workers.loaded_index as li

def test_skim_work(data_dir):
    index_dir = util.get_index_dir(data_dir)

    # delete the index if it exists already
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    assert not os.path.exists(index_dir)

    # build the index
    indexer = IndexBuilder(data_dir)
    indexer.build_index()
    idx = Index(data_dir)

    li.pubmed_path = data_dir
    li.the_index = idx

    # test SKiM with only A-B terms
    result = work.km_work_all_vs_all({'a_terms': ['cancer'], 'b_terms': ['test']})
    assert len(result) == 1
    assert 'c_term' not in result[0]
    assert result[0]['ab_count'] > 0

    # test SKiM with A-B-C terms
    result = work.km_work_all_vs_all({'a_terms': ['cancer'], 'b_terms': ['test'], 'c_terms': ['coffee'], 'top_n': 50, 'ab_fet_threshold': 0.8, 'query_knowledge_graph': True})
    assert len(result) == 1
    assert result[0]['c_term'] == 'coffee'
    assert result[0]['ab_count'] > 0
    assert result[0]['bc_count'] > 0
    assert result[0]['ab_relationship'] == 'neo4j connection error'
    assert result[0]['bc_relationship'] == 'neo4j connection error'