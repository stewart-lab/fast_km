from ..src import km_util as util
import pytest

def test_assert():
    print("test output")
    util.report_progress(1, 2)
    print("\ntest output 2")
    assert 4 == 2 + 2
    

def test_assert_2():
    assert True

def test_assert_3():
    assert False