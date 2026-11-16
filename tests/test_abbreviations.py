import collections
import json
import os
import shutil
import pytest
import src.indexing.abbreviations as abbreviations
import src.indexing.synonyms as synonyms
from src.documents.document import Document
from src.indexing.index import Index

def test_find_definitions():
    assert abbreviations.find_definitions("Infection with human papillomavirus (HPV) causes cancer.") == [("HPV", "human papillomavirus")]
    assert abbreviations.find_definitions("herpes simplex virus type 2 (HSV-2)") == [("HSV-2", "herpes simplex virus type 2")]
    assert abbreviations.find_definitions("levels of interleukin-6 (IL-6) rose") == [("IL-6", "interleukin-6")]
    assert abbreviations.find_definitions("monoclonal antibodies (mAbs)") == [("mAbs", "monoclonal antibodies")]

    # the long form stays within its clause
    assert abbreviations.find_definitions("We used mass spectrometry. Patients (MS) were") == []
    # not abbreviations
    assert abbreviations.find_definitions("results (see Table 1) were significant (p < 0.05, n = 30)") == []
    assert abbreviations.find_definitions("a group of mice (treated) was") == []

def test_select_groups():
    counts = collections.Counter({
        ("HPV", "human papillomavirus"): 90,
        ("HPV", "human papilloma virus"): 8,
        ("HPV", "human papillomaviruses"): 5,
        ("HPV", "hepatic portal vein"): 2,
        # ambiguous
        ("MS", "multiple sclerosis"): 60,
        ("MS", "mass spectrometry"): 40,
        # too rare to trust
        ("XQZ", "x quantum zone"): 3,
    })
    groups = abbreviations.select_groups(counts)
    assert groups == [["HPV", "human papillomavirus", "human papilloma virus", "human papillomaviruses"]]

def test_select_groups_merges_spellings_of_one_meaning():
    counts = collections.Counter({
        ("AIDS", "acquired immunodeficiency syndrome"): 70,
        ("AIDS", "acquired immune deficiency syndrome"): 30,
        ("HSV-2", "herpes simplex virus type 2"): 60,
        ("HSV-2", "herpes simplex virus 2"): 40,
    })
    assert abbreviations.select_groups(counts) == [
        ["AIDS", "acquired immunodeficiency syndrome", "acquired immune deficiency syndrome"],
        ["HSV-2", "herpes simplex virus type 2", "herpes simplex virus 2"],
    ]

def test_select_groups_compares_short_forms_in_lowercase():
    # 'PSA' and 'PsA' are both 'psa' in the index, so 'psa' is ambiguous
    counts = collections.Counter({
        ("PSA", "prostate specific antigen"): 70,
        ("PsA", "psoriatic arthritis"): 30,
    })
    assert abbreviations.select_groups(counts) == []

@pytest.fixture
def data_dir():
    path = os.path.join(os.getcwd(), "tests", "tmp", "abbreviations")
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    synonyms.save_synonym_table(path, 9999, [])
    synonyms._synonym_cache.clear()
    synonyms._last_checked.clear()
    synonyms._verb_cache.clear()
    synonyms.get_variants.cache_clear()
    yield path
    shutil.rmtree(path, ignore_errors=True)

def test_abbreviations_are_mined_when_indexing(data_dir: str, monkeypatch):
    monkeypatch.setattr(abbreviations, "MIN_DEFINITIONS", 2)

    idx = Index(data_dir, expand_synonyms=True)
    idx.add_or_update_documents([
        Document(pmid=1, pub_year=2020, title="a study", abstract="human papillomavirus (HPV) in women", origin="test.xml.gz"),
        Document(pmid=2, pub_year=2020, title="a study", abstract="human papillomavirus (HPV) vaccines", origin="test.xml.gz"),
        Document(pmid=3, pub_year=2020, title="a study", abstract="HPV in men", origin="test.xml.gz"),
        Document(pmid=4, pub_year=2020, title="a study", abstract="cervical lesions and human papillomavirus", origin="test.xml.gz"),
    ])
    for _progress in idx.index_documents():
        pass

    assert os.path.exists(os.path.join(data_dir, abbreviations.ABBREVIATIONS_FILENAME))
    assert idx.search_documents("HPV") == {1, 2, 3, 4}
    idx.close()

def test_new_abbreviations_are_picked_up_without_restarting(data_dir: str):
    assert "human papillomavirus" not in synonyms.expand_term("HPV", data_dir)

    synonyms.save_abbreviation_table(data_dir, [["HPV", "human papillomavirus"]])
    # make sure the mtime differs on filesystems with coarse timestamps
    mtime = os.path.getmtime(os.path.join(data_dir, abbreviations.ABBREVIATIONS_FILENAME)) + 10
    os.utime(os.path.join(data_dir, abbreviations.ABBREVIATIONS_FILENAME), (mtime, mtime))

    assert "human papillomavirus" in synonyms.expand_term("HPV", data_dir)

def test_curated_synonyms_win_over_mined_abbreviations(data_dir: str):
    synonyms.save_synonym_table(data_dir, 9999, [["Acquired Immunodeficiency Syndrome", "AIDS"]], gene_symbols=["AES"])
    synonyms.save_abbreviation_table(data_dir, [["AIDS", "aid disease"], ["AEs", "adverse events"], ["HPV", "human papillomavirus"]])
    synonyms._synonym_cache.clear()

    # MeSH already defines 'AIDS'
    assert "aid disease" not in synonyms.get_variants("AIDS", data_dir)
    # 'AES' is a (former) gene symbol, and 'AEs' (adverse events) lowercases to the same term
    assert "adverse events" not in synonyms.get_variants("AEs", data_dir)
    assert "human papillomavirus" in synonyms.get_variants("HPV", data_dir)

def test_hgnc_symbols():
    tsv = "\n".join([
        "hgnc_id\tsymbol\tname\tstatus\talias_symbol\tprev_symbol",
        'HGNC:1\tTLE5\tTLE family member 5\tApproved\tGRG5\t"AES|ESP1"',
        'HGNC:2\tOLD1\told gene 1\tEntry Withdrawn\t\t',
    ])
    assert synonyms._hgnc_symbols(tsv) == ["AES", "ESP1", "TLE5"]

def test_select_groups_keeps_members_of_a_series_apart():
    counts = collections.Counter({
        ("Ang I", "angiotensin i"): 50,
        ("Ang I", "angiotensin ii"): 50,
        ("HSV-2", "herpes simplex virus type 2"): 60,
        ("HSV-2", "herpes simplex virus 2"): 40,
    })
    # 'angiotensin ii' is a different meaning, so 'Ang I' is ambiguous
    assert abbreviations.select_groups(counts) == [["HSV-2", "herpes simplex virus type 2", "herpes simplex virus 2"]]

def test_mined_short_forms_are_only_searched_when_typed(data_dir: str):
    synonyms.save_abbreviation_table(data_dir, [["S", "sulfur"], ["Mg", "magnesium"],
                                                ["SPRINT", "systolic blood pressure intervention trial"]])
    synonyms._synonym_cache.clear()

    # 's' and 'mg' are also a letter and a unit in the index
    assert synonyms.get_variants("sulfur", data_dir) == ["sulfur", "sulfurs"]
    assert "mg" not in synonyms.get_variants("magnesium", data_dir)
    assert "magnesium" in synonyms.get_variants("Mg", data_dir)
    # a short form is only expanded when written the way papers write it
    assert "systolic blood pressure intervention trial" in synonyms.get_variants("SPRINT", data_dir)
    assert synonyms.get_variants("sprint", data_dir) == ["sprint", "sprints"]
    assert "magnesium" not in synonyms.get_variants("MG", data_dir)

def test_select_groups_keeps_different_words_apart():
    counts = collections.Counter({
        ("CLD", "chronic liver disease"): 55,
        ("CLD", "chronic lung disease"): 45,
    })
    # the letters are alike, but liver and lung are different words, so 'CLD' is ambiguous
    assert abbreviations.select_groups(counts) == []

def test_select_groups_counts_short_forms_as_the_index_stores_them():
    counts = collections.Counter({
        ("X-1", "xenon one"): 45, ("X-1", "xylose isomerase"): 5,
        ("X 1", "xylose isomerase"): 45, ("X 1", "xenon one"): 5,
    })
    # 'X-1' and 'X 1' are both 'x 1' in the index, where they are split between two meanings
    assert abbreviations.select_groups(counts) == []

def test_long_forms_do_not_start_in_the_title(data_dir: str, monkeypatch):
    monkeypatch.setattr(abbreviations, "MIN_DEFINITIONS", 1)
    _index(data_dir, [Document(pmid=1, pub_year=2020, title="Vaccination outcomes", abstract="(VO) were good", origin="test.xml.gz")])
    assert synonyms.read_abbreviation_table(data_dir) == []

def test_abbreviations_are_only_mined_where_expansion_is_used(data_dir: str):
    os.remove(os.path.join(data_dir, synonyms.SYNONYMS_FILENAME))
    _index(data_dir, [Document(pmid=1, pub_year=2020, title="a study", abstract="human papillomavirus (HPV)", origin="test.xml.gz")])
    assert not os.path.exists(os.path.join(data_dir, abbreviations.ABBREVIATIONS_FILENAME))

def test_abbreviations_in_an_old_format_are_rebuilt(data_dir: str, monkeypatch):
    monkeypatch.setattr(abbreviations, "MIN_DEFINITIONS", 1)
    _index(data_dir, [Document(pmid=1, pub_year=2020, title="a study", abstract="human papillomavirus (HPV)", origin="test.xml.gz")])
    with open(os.path.join(data_dir, abbreviations.ABBREVIATIONS_FILENAME), "w") as f:
        json.dump({"format": 0, "groups": []}, f)

    abbreviations.update_abbreviation_table(data_dir, changed_tables=[])
    assert synonyms.read_abbreviation_table(data_dir) == [["HPV", "human papillomavirus"]]

def _index(data_dir: str, documents: list[Document]):
    idx = Index(data_dir)
    idx.add_or_update_documents(documents)
    for _progress in idx.index_documents():
        pass
    idx.close()
