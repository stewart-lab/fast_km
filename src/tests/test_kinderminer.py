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

def test_chisq_pvalue():
    table = [[10, 3000], [2000, 10000000]]
    pvalue = km.chi_square(table)
    assert pvalue == pytest.approx(2.583e-30, abs=1e-30)

    table = [[1, 3000], [2000, 10000000]]
    pvalue = km.chi_square(table)
    assert pvalue == 1

    table = [[0, 100], [0, 10000000]]
    pvalue = km.chi_square(table)
    assert pvalue == 1

def test_text_sanitation():
    text = 'Testing123****.'
    sanitized_text = index.sanitize_term(text)
    assert sanitized_text == 'testing123'

    text = 'The quick brown fox ' + index.logical_or + ' jumped over the lazy dog.'
    sanitized_text = index.sanitize_term(text)
    assert sanitized_text == 'jumped over the lazy dog' + index.logical_or + 'the quick brown fox'

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
    assert len(lung_pmids) == 110
    assert len(tissue_pmids) == 234

    # test FET table construction
    fet_table = km.get_contingency_table(tissue_pmids, lung_pmids, idx.n_articles())
    assert fet_table == [
        [16, 218], 
        [94, 3811]
    ]

    # test censor year
    # also tests that the article title is queried properly
    km_result = km.kinderminer_search('patients undergoing pancreaticoduodenectomy', 'somatostatin', idx, censor_year_upper=2020, return_pmids=True)
    assert km_result['len(a_term_set)'] == 1
    assert km_result['n_articles'] == 7
    km_result = km.kinderminer_search('patients undergoing pancreaticoduodenectomy', 'somatostatin', idx, censor_year_upper=2020, return_pmids=True)
    assert km_result['len(a_term_set)'] == 1
    assert km_result['n_articles'] == 7

    # test KM query results
    km_result = km.kinderminer_search('significant', 'cancer', idx, return_pmids=True)
    assert km_result['pvalue'] == pytest.approx(0.507139, abs=1e-6)
    assert km_result['len(a_term_set)'] == 679
    assert km_result['len(b_term_set)'] == 303
    assert km_result['sort_ratio'] == pytest.approx(0.165016, abs=1e-6)
    assert km_result['n_articles'] == 4139
    assert len(km_result['pmid_intersection']) == 50

    # test multi-word term
    km_result = km.kinderminer_search('a significant', 'cancer', idx, return_pmids=True)
    assert km_result['pvalue'] == pytest.approx(0.931653, abs=1e-6)
    assert km_result['len(a_term_set)'] == 217
    assert km_result['len(b_term_set)'] == 303
    assert km_result['sort_ratio'] == pytest.approx(0.036303, abs=1e-6)
    assert km_result['n_articles'] == 4139
    assert len(km_result['pmid_intersection']) == 11

    # test 'and', 'or' logical operators
    km_result = km.kinderminer_search('skin', 'cancer' + index.logical_or + 'carcinoma', idx, return_pmids=True)
    assert km_result['pvalue'] == pytest.approx(0.753457, abs=1e-6)
    assert km_result['len(a_term_set)'] == 76
    assert km_result['len(b_term_set)'] == 337
    assert km_result['sort_ratio'] == pytest.approx(0.014836, abs=1e-6)
    assert km_result['n_articles'] == 4139
    assert km_result['pmid_intersection'] == [34579370, 34580336, 34581683, 34579963, 34582109]

    km_result = km.kinderminer_search('skin' + index.logical_and + 'treatment', 'cancer', idx, return_pmids=True)
    assert km_result['pvalue'] == pytest.approx(0.329304, abs=1e-6)
    assert km_result['len(a_term_set)'] == 16
    assert km_result['len(b_term_set)'] == 303
    assert km_result['sort_ratio'] == pytest.approx(0.006600, abs=1e-6)
    assert km_result['n_articles'] == 4139
    assert km_result['pmid_intersection'] == [34580336, 34582109]

def test_prediction_score():
    total_n = 33810017
    a_and_b = 437
    b_not_a = 1058 - a_and_b
    a_not_b = 1270220
    not_a_not_b = total_n - a_and_b - b_not_a - a_not_b

    table = [[a_and_b, a_not_b],
            [b_not_a, not_a_not_b]]

    fet = km.fisher_exact(table)
    ratio = km.get_sort_ratio(table)
    pred_score = km.get_prediction_score(fet, ratio)
    assert pred_score == 2.0