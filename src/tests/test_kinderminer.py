import pytest
import os
import shutil
from indexing.index import Index
from indexing.index_builder import IndexBuilder
from workers import kinderminer as km
from indexing import km_util as util
import indexing.index as index
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

def test_text_sanitation():
    text = 'Testing123****.'
    sanitized_text = index.sanitize_term(text)
    assert sanitized_text == 'testing123'

    text = 'The quick brown fox / jumped over the lazy dog.'
    sanitized_text = index.sanitize_term(text)
    assert sanitized_text == 'jumped over the lazy dog/the quick brown fox'

    text = 'This&is&a&test.'
    sanitized_text = index.sanitize_term(text)
    assert sanitized_text == 'a&is&test&this'

def test_kinderminer(data_dir):
    index_dir = util.get_index_dir(data_dir)

    # delete the index if it exists already
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    assert not os.path.exists(index_dir)

    # build the index
    indexer = IndexBuilder(data_dir)
    indexer.build_index()
    idx = Index(data_dir)

    # test index querying
    lung_pmids = idx._query_index('lung')
    tissue_pmids = idx._query_index('tissue')
    assert len(lung_pmids) == 109
    assert len(tissue_pmids) == 234

    # test FET table construction
    fet_table = km.get_contingency_table(tissue_pmids, lung_pmids, idx.n_articles())
    assert fet_table == [
        [16, 218], 
        [93, 3812]
    ]

    # test censor year
    # also tests that the article title is queried properly
    km_result = km.kinderminer_search('patients undergoing pancreaticoduodenectomy', 'somatostatin', idx, censor_year=2020, return_pmids=True)
    assert km_result['len(a_term_set)'] == 1
    assert km_result['n_articles'] == 6
    km_result = km.kinderminer_search('patients undergoing pancreaticoduodenectomy', 'somatostatin', idx, censor_year=2020, return_pmids=True)
    assert km_result['len(a_term_set)'] == 1
    assert km_result['n_articles'] == 6

    # test KM query results
    km_result = km.kinderminer_search('significant', 'cancer', idx, return_pmids=True)
    assert km_result['pvalue'] == pytest.approx(0.486007, abs=1e-6)
    assert km_result['len(a_term_set)'] == 679
    assert km_result['len(b_term_set)'] == 301
    assert km_result['sort_ratio'] == pytest.approx(0.166112, abs=1e-6)
    assert km_result['n_articles'] == 4139
    assert len(km_result['pmid_intersection']) == 50

    # test multi-word term
    km_result = km.kinderminer_search('a significant', 'cancer', idx, return_pmids=True)
    assert km_result['pvalue'] == pytest.approx(0.928095, abs=1e-6)
    assert km_result['len(a_term_set)'] == 217
    assert km_result['len(b_term_set)'] == 301
    assert km_result['sort_ratio'] == pytest.approx(0.036544, abs=1e-6)
    assert km_result['n_articles'] == 4139
    assert len(km_result['pmid_intersection']) == 11

    # test 'and', 'or' logical operators
    km_result = km.kinderminer_search('skin', 'cancer/carcinoma', idx, return_pmids=True)
    assert km_result['pvalue'] == pytest.approx(0.748696, abs=1e-6)
    assert km_result['len(a_term_set)'] == 76
    assert km_result['len(b_term_set)'] == 335
    assert km_result['sort_ratio'] == pytest.approx(0.0149253, abs=1e-6)
    assert km_result['n_articles'] == 4139
    assert km_result['pmid_intersection'] == {34579370, 34580336, 34581683, 34579963, 34582109}

    km_result = km.kinderminer_search('skin&treatment', 'cancer', idx, return_pmids=True)
    assert km_result['pvalue'] == pytest.approx(0.326369, abs=1e-6)
    assert km_result['len(a_term_set)'] == 16
    assert km_result['len(b_term_set)'] == 301
    assert km_result['sort_ratio'] == pytest.approx(0.006644, abs=1e-6)
    assert km_result['n_articles'] == 4139
    assert km_result['pmid_intersection'] == {34580336, 34582109}