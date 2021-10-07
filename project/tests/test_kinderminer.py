import pytest
from ..src import kinderminer as km

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