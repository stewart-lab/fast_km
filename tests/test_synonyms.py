import io
import json
import os
import shutil
import time
import pytest
import src.indexing.synonyms as synonyms
from src.documents.document import Document
from src.indexing.index import Index

# a tiny stand-in for the MeSH synonym table, so tests don't download MeSH
MESH_GROUPS = [
    ["Breast Cancer", "Mammary Cancer", "Cancer of the Breast"],
    ["Acquired Immunodeficiency Syndrome", "AIDS"],
    ["Myocardial Infarction", "Heart Attack"],
]

@pytest.fixture
def data_dir():
    path = os.path.join(os.getcwd(), "tests", "tmp", "synonyms")
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    synonyms.save_synonym_table(path, 9999, MESH_GROUPS)
    synonyms._synonym_cache.clear()
    synonyms._last_checked.clear()
    synonyms._verb_cache.clear()
    synonyms.get_variants.cache_clear()
    yield path
    shutil.rmtree(path, ignore_errors=True)

def test_plural_and_singular(data_dir: str):
    assert synonyms.get_variants("tumor", data_dir) == ["tumor", "tumors"]
    assert synonyms.get_variants("tumors", data_dir) == ["tumors", "tumor"]
    assert synonyms.get_variants("mice", data_dir) == ["mice", "mouse"]

    # only the last word is inflected
    assert synonyms.get_variants("lung tumor", data_dir) == ["lung tumor", "lung tumors"]

def test_mesh_synonyms(data_dir: str):
    variants = synonyms.get_variants("breast cancer", data_dir)
    assert variants[0] == "breast cancer"
    assert "mammary cancer" in variants
    assert "cancer of the breast" in variants
    assert "breast cancers" in variants
    assert "mammary cancers" in variants

    # a plural query still finds the singular MeSH term's synonyms
    assert "myocardial infarction" in synonyms.get_variants("heart attacks", data_dir)

def test_acronyms_are_not_inflected(data_dir: str):
    variants = synonyms.get_variants("AIDS", data_dir)
    assert "aid" not in variants
    assert "acquired immunodeficiency syndrome" in variants

    # acronym synonyms are only searched for when the user wrote the acronym,
    # since lowercase acronyms collide with ordinary words
    variants = synonyms.get_variants("acquired immunodeficiency syndrome", data_dir)
    assert "aids" not in variants
    assert "aid" not in variants

def test_expand_composite_term(data_dir: str):
    expanded = synonyms.expand_term("tumor & (rat | heart attack)", data_dir)
    assert expanded == "(tumor|tumors)&((rat)|(heart attack|heart attacks|myocardial infarction|myocardial infarctions))"

def test_expansion_stays_under_subterm_limit(data_dir: str):
    term = "|".join(f"tumor{i} x" for i in range(60)) + "|breast cancer"
    expanded = synonyms.expand_term(term, data_dir)
    n_subterms = expanded.count("|") + 1
    assert n_subterms <= synonyms.MAX_SUBTERMS

def test_search_with_expansion(data_dir: str):
    idx = Index(data_dir)
    idx.add_or_update_documents([
        Document(pmid=1, pub_year=2020, title="breast cancer", abstract="a study", origin="test.xml.gz"),
        Document(pmid=2, pub_year=2020, title="breast cancers", abstract="a study", origin="test.xml.gz"),
        Document(pmid=3, pub_year=2020, title="mammary cancer", abstract="a study", origin="test.xml.gz"),
        Document(pmid=4, pub_year=2020, title="lung cancer", abstract="a study", origin="test.xml.gz"),
    ])
    for _progress in idx.index_documents():
        pass
    assert idx.search_documents("breast cancer") == {1}
    idx.close()

    idx = Index(data_dir, expand_synonyms=True)
    assert idx.search_documents("breast cancer") == {1, 2, 3}
    assert idx.search_documents("breast cancer & study") == {1, 2, 3}
    idx.close()

def _make_outdated(data_dir: str):
    synonyms.save_synonym_table(data_dir, 2000, MESH_GROUPS)
    long_ago = time.time() - synonyms.RETRY_REFRESH_AFTER_SECONDS - 60
    os.utime(os.path.join(data_dir, synonyms.SYNONYMS_FILENAME), (long_ago, long_ago))

def test_outdated_table_is_rebuilt(data_dir: str, monkeypatch):
    _make_outdated(data_dir)

    def fake_build(data_dir: str, year: int | None = None):
        synonyms.save_synonym_table(data_dir, 9999, [["Heart Attack", "Cardiac Infarction"]])

    monkeypatch.setattr(synonyms, "build_synonym_table", fake_build)
    assert "cardiac infarction" in synonyms.get_variants("heart attack", data_dir)

def test_outdated_table_is_kept_if_refresh_fails(data_dir: str, monkeypatch):
    _make_outdated(data_dir)

    attempts = []
    def failing_build(data_dir: str, year: int | None = None):
        attempts.append(1)
        raise synonyms.FastKmException("NLM unreachable")

    monkeypatch.setattr(synonyms, "build_synonym_table", failing_build)
    assert "myocardial infarction" in synonyms.get_variants("heart attack", data_dir)

    # the next job doesn't retry right away
    synonyms._synonym_cache.clear()
    synonyms.load_synonyms(data_dir)
    assert len(attempts) == 1

def test_parse_mesh_xml():
    xml = b"""<?xml version="1.0"?>
<Records>
<DescriptorRecord>
  <ConceptList>
   <Concept PreferredConceptYN="Y">
    <TermList>
     <Term><String>Breast Neoplasms</String></Term>
     <Term><String>Breast Tumors</String></Term>
     <Term><String>Neoplasms, Breast</String></Term>
    </TermList>
   </Concept>
   <Concept PreferredConceptYN="N">
    <TermList>
     <Term><String>Breast Cancer</String></Term>
     <Term><String>Mammary Cancer</String></Term>
     <Term><String>BC (Breast Cancer)</String></Term>
    </TermList>
   </Concept>
  </ConceptList>
</DescriptorRecord>
<SupplementalRecord>
  <ConceptList>
   <Concept PreferredConceptYN="Y">
    <TermList><Term><String>abemaciclib</String></Term></TermList>
   </Concept>
   <Concept PreferredConceptYN="N">
    <TermList>
     <Term><String>LY2835219</String></Term>
     <Term><String>LY-2835219</String></Term>
    </TermList>
   </Concept>
   <Concept PreferredConceptYN="N">
    <TermList><Term><String>Verzenio</String></Term></TermList>
   </Concept>
  </ConceptList>
</SupplementalRecord>
</Records>"""
    assert synonyms._parse_mesh_xml(io.BytesIO(xml)) == [
        # descriptor concepts stay separate, so 'breast cancer' doesn't broaden to 'breast tumors'
        ["Breast Neoplasms", "Breast Tumors"],
        ["Breast Cancer", "Mammary Cancer"],
        # a supplementary record's concepts are one substance
        ["abemaciclib", "LY2835219", "LY-2835219", "Verzenio"],
    ]

def test_codes_are_synonyms_but_word_acronyms_are_not(data_dir: str):
    synonyms.save_synonym_table(data_dir, 9999, [["abemaciclib", "LY2835219", "Verzenio"], ["Multiple Sclerosis", "MS"]])
    synonyms._synonym_cache.clear()
    variants = synonyms.get_variants("abemaciclib", data_dir)
    assert variants[0] == "abemaciclib"
    assert {"ly2835219", "ly 2835219", "verzenio"} <= set(variants)
    assert "ms" not in synonyms.get_variants("multiple sclerosis", data_dir)

def test_singulars_ending_in_s(data_dir: str):
    assert synonyms.get_variants("multiple sclerosis", data_dir) == ["multiple sclerosis", "multiple scleroses"]
    assert synonyms.get_variants("virus", data_dir) == ["virus", "viruses"]

def test_parse_hgnc_tsv():
    tsv = "\n".join([
        "hgnc_id\tsymbol\tname\tstatus\talias_symbol\tprev_symbol",
        'HGNC:1773\tCDK4\tcyclin dependent kinase 4\tApproved\tPSK-J3\t',
        'HGNC:1787\tCDKN2A\tcyclin dependent kinase inhibitor 2A\tApproved\t"p16|ARF|p1"\t"CDKN2|MLM"',
        'HGNC:9999\tFAKE1\tfake gene 1\tApproved\t"p1|CDKN2"\t',
        'HGNC:9998\tOLD1\told gene 1\tEntry Withdrawn\t\t',
    ])
    assert synonyms._parse_hgnc_tsv(tsv) == [
        ["CDK4", "cyclin dependent kinase 4", "PSK-J3"],
        # 'CDKN2' and 'p1' name two genes, so neither gene gets them,
        # and 'MLM' and 'ARF' read as ordinary words once lowercased
        ["CDKN2A", "cyclin dependent kinase inhibitor 2A", "p16"],
        ["FAKE1", "fake gene 1"],
    ]

def test_gene_synonyms(data_dir: str):
    synonyms.save_synonym_table(data_dir, 9999, [["CDKN2A", "cyclin dependent kinase inhibitor 2A", "MLM", "ARF", "p16"]])
    synonyms._synonym_cache.clear()

    variants = synonyms.get_variants("CDKN2A", data_dir)
    assert "cyclin dependent kinase inhibitor 2a" in variants
    assert "p16" in variants
    # all-letter aliases are ordinary words once lowercased ('arf' is also acute renal failure)
    assert "arf" not in variants
    assert "mlm" not in variants

    assert "cdkn2a" in synonyms.get_variants("p16", data_dir)

def test_gene_aliases_must_look_like_identifiers():
    tsv = "\n".join([
        "hgnc_id\tsymbol\tname\tstatus\talias_symbol\tprev_symbol",
        'HGNC:1\tWDHD1\tWD repeat and HMG-box DNA binding protein 1\tApproved\t"AND-1|CTF4"\t',
        'HGNC:2\tELL\telongation factor for RNA polymerase II\tApproved\t"Men|ELL1"\t',
        'HGNC:3\tPLXNA3\tplexin A3\tApproved\t"6-3|XAP-6"\t',
        'HGNC:4\tCXCL8\tC-X-C motif chemokine ligand 8\tApproved\t"IL-8|NAP-1"\t',
        # 'COX-2' is spelled differently from PTGS2's 'COX2', but names the same thing
        'HGNC:5\tPTGER2\tprostaglandin E receptor 2\tApproved\t"COX-2|EP2"\t',
        'HGNC:6\tPTGS2\tprostaglandin-endoperoxide synthase 2\tApproved\t"COX2|PHS-2"\t',
    ])
    assert synonyms._parse_hgnc_tsv(tsv) == [
        ["WDHD1", "WD repeat and HMG-box DNA binding protein 1", "CTF4"],
        ["ELL", "elongation factor for RNA polymerase II", "ELL1"],
        ["PLXNA3", "plexin A3", "XAP-6"],
        ["CXCL8", "C-X-C motif chemokine ligand 8", "IL-8", "NAP-1"],
        ["PTGER2", "prostaglandin E receptor 2", "EP2"],
        ["PTGS2", "prostaglandin-endoperoxide synthase 2", "PHS-2"],
    ]

def test_fields_of_study_are_not_singularized(data_dir: str):
    assert synonyms.get_variants("genetics", data_dir) == ["genetics"]
    assert "antibiotics" in synonyms.get_variants("antibiotic", data_dir)

def test_hyphenated_names(data_dir: str):
    # 'IL-6' is indexed as 'il 6'
    assert synonyms.get_variants("IL6", data_dir) == ["il6", "il 6"]
    assert synonyms.get_variants("IL-6", data_dir) == ["il 6", "il6"]
    # a single letter would match ordinary text, like 'p 53' in 'p = 0.53'
    assert synonyms.get_variants("p53", data_dir) == ["p53"]
    # not when the letters are a word often followed by a number: 'the ratio is 2', 'stage 4'
    assert synonyms.get_variants("IS2", data_dir) == ["is2"]
    assert synonyms.get_variants("stage4", data_dir) == ["stage4"]

def test_typed_acronyms_only_match_acronyms(data_dir: str):
    synonyms.save_synonym_table(data_dir, 9999, [["Id", "Ids"], ["IDS", "iduronate 2-sulfatase", "ID2S"],
                                                 ["Acquired Immunodeficiency Syndrome", "AIDS"]])
    synonyms._synonym_cache.clear()
    assert "id" not in synonyms.get_variants("IDS", data_dir)
    assert "iduronate 2 sulfatase" in synonyms.get_variants("IDS", data_dir)
    assert "iduronate 2 sulfatase" not in synonyms.get_variants("ids", data_dir)
    # an acronym written in lowercase still finds its synonyms when it isn't also a word
    assert "acquired immunodeficiency syndrome" in synonyms.get_variants("aids", data_dir)

def test_old_table_format_is_rebuilt(data_dir: str, monkeypatch):
    with open(os.path.join(data_dir, synonyms.SYNONYMS_FILENAME), "w") as f:
        json.dump({"year": 9999, "groups": [["heart attack", "myocardial infarction"]], "acronyms": []}, f)

    def fake_build(data_dir: str, year: int | None = None):
        synonyms.save_synonym_table(data_dir, 9999, [["Heart Attack", "Cardiac Infarction"]])

    monkeypatch.setattr(synonyms, "build_synonym_table", fake_build)
    assert "cardiac infarction" in synonyms.get_variants("heart attack", data_dir)

def test_phrases_with_function_words_are_not_inflected(data_dir: str):
    assert synonyms.get_variants("cancer of the cervix", data_dir) == ["cancer of the cervix"]
    assert synonyms.get_variants("lung tumor", data_dir) == ["lung tumor", "lung tumors"]

def test_words_typed_in_capitals(data_dir: str):
    synonyms.save_synonym_table(data_dir, 9999, [["Abacavir", "Ziagen"], ["IDS", "iduronate 2-sulfatase"], ["Id", "Ids"]])
    synonyms._synonym_cache.clear()
    # drug lists are often in capitals; a word only known as a word is still expanded and inflected
    assert synonyms.get_variants("ABACAVIR", data_dir) == synonyms.get_variants("abacavir", data_dir)
    assert "ziagen" in synonyms.get_variants("ABACAVIR", data_dir)
    # but a capitalized term that is also an acronym is still only an acronym
    assert "id" not in synonyms.get_variants("IDS", data_dir)

def test_stray_parentheses_are_ignored(data_dir: str):
    # e.g. 'PROTEASE)' in a pasted drug list; the expanded query must still be valid
    assert synonyms.expand_term("tumor)", data_dir) == synonyms.expand_term("tumor", data_dir)
    assert synonyms.expand_term("(tumor", data_dir) == synonyms.expand_term("tumor", data_dir)
    # parentheses used as text, e.g. '(+)-aldosterone'
    assert synonyms.expand_term("(+)-tumor", data_dir) == synonyms.expand_term("tumor", data_dir)
    assert synonyms.expand_term("heart attack (acute)", data_dir) == synonyms.expand_term("heart attack acute", data_dir)
    # but grouping parentheses are kept
    assert synonyms.expand_term("(tumor|rat)&heart attack", data_dir).startswith("((tumor|tumors)|(rat))&")

def test_table_from_an_older_mesh_year_is_not_rebuilt_on_every_search(data_dir: str, monkeypatch):
    # e.g. in January, before this year's MeSH is out, the build falls back to last year's
    _make_outdated(data_dir)
    attempts = []
    def fallback_build(data_dir: str, year: int | None = None):
        attempts.append(1)
        synonyms.save_synonym_table(data_dir, 2000, MESH_GROUPS)

    monkeypatch.setattr(synonyms, "build_synonym_table", fallback_build)
    synonyms.load_synonyms(data_dir)
    synonyms._synonym_cache.clear()
    synonyms.load_synonyms(data_dir)
    assert len(attempts) == 1

def test_indexing_refreshes_the_synonym_table(data_dir: str, monkeypatch):
    _make_outdated(data_dir)
    def fake_build(data_dir: str, year: int | None = None):
        synonyms.save_synonym_table(data_dir, 9999, [["Heart Attack", "Cardiac Infarction"]])
    monkeypatch.setattr(synonyms, "build_synonym_table", fake_build)

    idx = Index(data_dir)
    idx.add_or_update_documents([Document(pmid=1, pub_year=2020, title="a study", abstract="heart attack", origin="test.xml.gz")])
    for _progress in idx.index_documents():
        pass
    idx.close()
    with open(os.path.join(data_dir, synonyms.SYNONYMS_FILENAME)) as f:
        assert json.load(f)["year"] == 9999

def test_expanded_variants_are_not_cached_on_disk(data_dir: str):
    idx = Index(data_dir, expand_synonyms=True)
    idx.add_or_update_documents([Document(pmid=1, pub_year=2020, title="a study", abstract="cancer of the breast", origin="test.xml.gz")])
    for _progress in idx.index_documents():
        pass
    assert idx.search_documents("cancer of the breast") == {1}
    assert idx.search_documents("breast cancer") == {1}
    cached = {row[0] for row in idx._idx_cursor.execute("SELECT term FROM query_cache")}
    idx.close()
    # the user's phrase is cached, its synonyms aren't
    assert cached == {"cancer of the breast"}

def test_composite_terms_with_non_ascii_letters(data_dir: str):
    # with expansion a single term becomes a composite one; 'µ' (micro sign) isn't a valid
    # Python name as is, so it must not end up in the evaluated expression
    idx = Index(data_dir, expand_synonyms=True)
    idx.add_or_update_documents([Document(pmid=1, pub_year=2020, title="a study", abstract="the µ-opioid receptor", origin="test.xml.gz")])
    for _progress in idx.index_documents():
        pass
    assert idx.search_documents("µ-opioid receptor") == {1}
    assert idx.search_documents("µ-opioid receptor|kappa receptor") == {1}
    idx.close()

def test_plurals_used_mostly_as_verbs_are_left_out(data_dir: str):
    # 'leads' is nearly always the verb ('which leads to'), 'tumors' the plural noun ('of tumors')
    counts = {"it leads": 50, "which leads": 40, "the leads": 5, "of tumors": 80, "these tumors": 20, "which tumors": 1}
    def ngram_sizes(ngrams):
        return [counts.get(ngram, 0) for ngram in ngrams]

    assert synonyms.expand_term("lead", data_dir, ngram_sizes) == "(lead)"
    assert synonyms.expand_term("tumor", data_dir, ngram_sizes) == "(tumor|tumors)"
    # what the user typed is always searched
    assert synonyms.expand_term("leads", data_dir, ngram_sizes) == "(leads|lead)"
