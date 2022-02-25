import pytest
import os
import shutil
import time
from indexing.index import Index
from indexing.index_builder import IndexBuilder
from workers import kinderminer as km
from indexing import km_util as util
from .test_index_building import data_dir

def test_fisher_exact_test():
    # example shown in figure 1 of:
    # https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5543342/
    a_term_set = set(range(0, 2027)) # embryonic stem cell
    b_term_set = set(range(2012, 2071)) # NANOG
    total_set = set(range(0,17012366))

    table = km.get_contingency_table(a_term_set, b_term_set, len(total_set))
    assert table == [[15,2012],[44,17010295]]

    pvalue = km.fisher_exact(table)
    assert pvalue == pytest.approx(5.219e-46, abs=1e-46)

    sort_ratio = km.get_sort_ratio(table)
    assert sort_ratio == pytest.approx(15 / 59)

def test_kinderminer(data_dir):
    index_dir = util.get_index_dir(data_dir)

    # delete the index if it exists already
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    assert not os.path.exists(index_dir)

    # build the index
    indexer = IndexBuilder(data_dir)
    indexer.build_index()
    
    # run kinderminer query
    idx = Index(data_dir)
    km_result = km.kinderminer_search('cancer', 'brca1', idx, return_pmids=True)

    assert km_result['pmid_intersection'] == {34580114}

    km_or_result = km.kinderminer_search('cancer/carcinoma', 'brca1', idx)
    km_and_result = km.kinderminer_search('cancer&carcinoma', 'brca1', idx)

    # assertions
    assert km_or_result['len(a_term_set)'] > km_result['len(a_term_set)']
    assert km_and_result['len(a_term_set)'] < km_result['len(a_term_set)']

    assert km_or_result['len(b_term_set)'] == km_result['len(b_term_set)']
    assert km_and_result['len(b_term_set)'] == km_result['len(b_term_set)']

    # delete the index when the test is done
    #shutil.rmtree(index_dir)