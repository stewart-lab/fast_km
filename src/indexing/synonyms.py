"""
Query-time synonym expansion.

The index stores exact (lowercased) tokens, so 'breast cancer' does not match
'breast cancers' or 'breast neoplasms'. When expansion is enabled, each subterm
of a query is rewritten as an OR-group of its variants before searching:

    'breast cancer & CDK4'  ->  '(breast cancer|breast cancers|breast neoplasms|...)&(cdk4)'

Variants come from three sources:
  1. MeSH entry terms: if the subterm is a term of a MeSH concept, all terms of
     that concept are added. For descriptors, concepts are used rather than whole
     descriptors, since a descriptor also holds narrower concepts that would broaden
     the query (e.g. 'breast cancer' is its own concept under 'Breast Neoplasms').
     For supplementary records (mostly drugs and chemicals), the whole record is used,
     since its other concepts are brand names and codes for the same substance
     (e.g. 'abemaciclib', 'Verzenio', 'LY2835219').
  2. HGNC human gene names: a gene's official symbol, full name, previous symbols and
     aliases (e.g. 'CDK4', 'cyclin dependent kinase 4', 'PSK-J3'). Previous symbols and
     aliases are only used when they look like gene identifiers: they must name a single
     gene and contain both letters and digits, and no function words. This drops aliases
     that are ordinary words once lowercased ('Ray', 'Men', 'AND-1' -> 'and 1').
  3. Plural/singular inflection of the last word of each variant, unless the phrase has a
     function word in it ('cancer of the cervix'), since then the last word isn't the head noun.
  4. Abbreviations defined in the corpus itself, e.g. 'human papillomavirus (HPV)'; see abbreviations.py.
     They are only used when the short form is written the way papers write it ('HPV', 'Mg'), since
     many collide with ordinary words in the lowercase index ('SPRINT' trial vs 'sprint').
Names like 'IL6' also get their hyphenated spelling ('il 6', as 'IL-6' is indexed), and vice versa.

The synonym table is built from NLM's MeSH descriptor and supplementary concept XML
(the latter covers newer drugs and chemicals) and HGNC's gene table, and stored in the
data dir. It is rebuilt when a newer MeSH year is released.
"""

import csv
import dataclasses
import fcntl
import functools
import gzip
import json
import os
import re
import tempfile
import time
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable
import inflect
import src.indexing.indexing_util as util
from src.fast_km_exception import FastKmException

MESH_URLS = [
    "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/desc{year}.gz",  # descriptors
    "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/supp{year}.gz",  # supplementary concepts
]
HGNC_URL = "https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt"
SYNONYMS_FILENAME = "synonyms.json"
ABBREVIATIONS_FILENAME = "abbreviations.json"  # built by abbreviations.py when documents are indexed
RELOAD_AFTER_SECONDS = 24 * 60 * 60            # workers reload the table at least this often
RETRY_REFRESH_AFTER_SECONDS = 7 * 24 * 60 * 60  # how long to wait before retrying an out of date table's refresh
CHECK_FILES_EVERY_SECONDS = 60                  # how often workers check whether the table files changed
TABLE_FORMAT = 4          # bump when the stored layout changes, so old tables are rebuilt
ABBREVIATIONS_FORMAT = 1  # likewise for the abbreviations table
MIN_GENE_ALIAS_LENGTH = 3

# search_documents() rejects terms with more than this many subterms
MAX_SUBTERMS = 100
MAX_NGRAM_N = 20
MAX_VARIANTS_PER_SUBTERM = 30
POSSESSIVE_SHARE = 0.2  # see is_usually_possessive

# words that mark a phrase's last word as not its head noun, and that rule out gene aliases
FUNCTION_WORDS = {
    "a", "an", "the", "and", "or", "of", "in", "on", "at", "to", "for", "with", "without", "by",
    "from", "as", "via", "into", "per", "between", "among", "against", "during", "after", "before", "within",
}

_inflect_engine = inflect.engine()
_operator_split = re.compile(r"([&|()])")

def expand_term(term: str, data_dir: str, ngram_sizes: Callable[[list[str]], list[int]] | None = None) -> str:
    """Rewrite each subterm of a (possibly composite) query term as an OR-group of its variants.
    ngram_sizes gives how common bi-grams are in the corpus; if given, plurals that are mostly
    used as verbs are left out (see is_mostly_verb), and possessive forms that papers mostly use
    are added (see is_usually_possessive)."""
    return join_expansion(expansion(term, data_dir, ngram_sizes))

def join_expansion(parts: list[str | tuple[str, list[str]]]) -> str:
    """The query for an expansion(): each subterm as an OR-group of its variants."""
    return "".join(part if isinstance(part, str) else "(" + "|".join(part[1]) + ")" for part in parts)

def expansion(term: str, data_dir: str, ngram_sizes: Callable[[list[str]], list[int]] | None = None) -> list[str | tuple[str, list[str]]]:
    """The parts of a (possibly composite) term: operators as strings, and each subterm as
    (subterm as typed, the variants it is searched as). This is what expand_term() searches, so
    it can be shown to users."""
    # parentheses used as text in term lists ('(+)-aldosterone', 'PROTEASE)') aren't split on
    term = util.remove_text_parentheses(term)
    parts = [p for p in _operator_split.split(term) if p.strip()]
    if not any(p not in util.logical_operators for p in parts):
        return [term]
    # a term already over the subterm limit isn't expanded, so search_documents() rejects it
    # the same way as without expansion, rather than expansion quietly dropping some of it
    if len(set(util.get_subterms(util.sanitize_term_for_search(term)))) > MAX_SUBTERMS:
        return [term]

    _drop_stale_cache(data_dir)
    part_variants = []
    for part in parts:
        if part in util.logical_operators:
            continue
        variants = get_variants(part, data_dir)
        if ngram_sizes is not None:
            variants = _drop_verb_forms(variants, data_dir, ngram_sizes)
            variants = _add_possessives(variants, data_dir, ngram_sizes)
        part_variants.append(variants)

    # in a list of alternatives ('CTLA4|CD152|...'), a variant that an earlier alternative already
    # searches adds nothing: drop it, so it neither takes a slot nor is searched twice
    if all(p == "|" for p in parts if p in util.logical_operators):
        subterms = [p.strip() for p in parts if p not in util.logical_operators]
        out = []
        for subterm, variants in zip(subterms, _without_repeats(part_variants)):
            if variants:
                out += (["|"] if out else []) + [(subterm, variants)]
        return out

    # stay under the subterm limit in search_documents(), or the whole query returns nothing
    caps = iter(_share_variant_budget([len(v) for v in part_variants]))
    variants = iter(part_variants)
    out = []
    for part in parts:
        if part in util.logical_operators:
            out.append(part)
        else:
            out.append((part.strip(), next(variants)[:next(caps)]))
    return out

def _without_repeats(part_variants: list[list[str]]) -> list[list[str]]:
    """The variants each alternative searches, with no variant searched twice and at most
    MAX_SUBTERMS in all. Alternatives take turns picking their next variant not yet taken (at most
    MAX_VARIANTS_PER_SUBTERM each), so every alternative's own name comes first, even when an
    earlier alternative's synonym list also contains it ('EPCAM' lists 'CD326' among ~60 names)."""
    taken = set[str]()
    kept = [[] for _ in part_variants]
    next_i = [0] * len(part_variants)
    total, picked = 0, True
    while picked and total < MAX_SUBTERMS:
        picked = False
        for i, variants in enumerate(part_variants):
            if total >= MAX_SUBTERMS or len(kept[i]) >= MAX_VARIANTS_PER_SUBTERM:
                continue
            while next_i[i] < len(variants) and variants[next_i[i]] in taken:
                next_i[i] += 1
            if next_i[i] < len(variants):
                kept[i].append(variants[next_i[i]])
                taken.add(variants[next_i[i]])
                next_i[i] += 1
                total += 1
                picked = True
    return kept

def _share_variant_budget(counts: list[int]) -> list[int]:
    """How many variants each subterm may keep: at most MAX_VARIANTS_PER_SUBTERM each, and
    MAX_SUBTERMS in all. Subterms with few variants take only what they have, and the slots they
    don't use go to the others: in 'CTLA4|CD152|...|cytotoxic T-lymphocyte associated protein 4',
    the long name can keep more than an equal share of 100."""
    wanted = [min(n, MAX_VARIANTS_PER_SUBTERM) for n in counts]
    caps = [0] * len(counts)
    budget, open_ = MAX_SUBTERMS, list(range(len(counts)))
    while open_:
        share = max(1, budget // len(open_))
        satisfied = [i for i in open_ if wanted[i] <= share]
        if not satisfied:
            for i in open_:
                caps[i] = share
            break
        for i in satisfied:
            caps[i] = wanted[i]
            budget -= wanted[i]
        open_ = [i for i in open_ if i not in satisfied]
    return caps

# words that come before a verb ('it leads', 'which binds') and before a plural noun ('of vaccines', 'these tumors')
_VERB_CONTEXT = ("it", "which", "also")
_NOUN_CONTEXT = ("of", "these", "the", "with", "for")

def is_mostly_verb(word: str, ngram_sizes: Callable[[list[str]], list[int]]) -> bool:
    """Whether a word ending in -s is used more as a verb than as a plural noun in the corpus.
    'lead' (the metal) would otherwise also match 'leads', which is nearly always 'leads to'."""
    verb = sum(ngram_sizes([f"{w} {word}" for w in _VERB_CONTEXT]))
    noun = sum(ngram_sizes([f"{w} {word}" for w in _NOUN_CONTEXT]))
    return verb > noun

def _drop_verb_forms(variants: list[str], data_dir: str, ngram_sizes: Callable[[list[str]], list[int]]) -> list[str]:
    # only plurals we added; the subterm itself is always kept
    kept = variants[:1]
    for variant in variants[1:]:
        last = variant.split(" ")[-1]
        added_plural = last.endswith("s") and any(other in variants for other in _inflections(variant))
        if added_plural:
            key = (data_dir, last)
            if key not in _verb_cache:
                _verb_cache[key] = is_mostly_verb(last, ngram_sizes)
            if _verb_cache[key]:
                continue
        kept.append(variant)
    return kept

_verb_cache = dict[tuple[str, str], bool]()  # cleared with the synonym table, so it follows the corpus

def is_usually_possessive(word: str, next_word: str, ngram_sizes: Callable[[list[str]], list[int]]) -> bool:
    """Whether papers often write a word as a possessive: 'Alzheimer's disease' is indexed as
    'alzheimer s disease', so a search for 'Alzheimer disease' misses most papers. Eponyms are
    written 'X's' at least POSSESSIVE_SHARE as often as 'X <next word>' ('alzheimer s' 10x as often
    as 'alzheimer disease'); other words almost never are ('breast s' vs 'breast cancer', 0.0003x)."""
    possessive, plain = ngram_sizes([f"{word} s", f"{word} {next_word}"])
    return possessive > 0 and possessive >= POSSESSIVE_SHARE * plain

def _add_possessives(variants: list[str], data_dir: str, ngram_sizes: Callable[[list[str]], list[int]]) -> list[str]:
    # only words of the typed subterm, judged with the word after them there, so a rare variant
    # ('breast cancers') can't tip it, and synonyms don't get odd forms ('lewy body s ...')
    if not variants:
        return variants  # the subterm was only punctuation
    possessive_words = dict[str, bool]()
    words = variants[0].split(" ")
    for word, next_word in zip(words, words[1:]):
        if word not in possessive_words and next_word != "s" and word.isalpha() and len(word) >= 3 and word not in FUNCTION_WORDS:
            key = (data_dir, word, next_word)
            if key not in _possessive_cache:
                _possessive_cache[key] = is_usually_possessive(word, next_word, ngram_sizes)
            possessive_words[word] = _possessive_cache[key]

    def possessive_of(variant: str) -> str | None:
        words = variant.split(" ")
        if "s" in words:
            return None  # already possessive
        for i, word in enumerate(words[:-1]):
            if possessive_words.get(word):
                return " ".join(words[:i + 1] + ["s"] + words[i + 1:])
        return None

    # the typed subterm's possessive comes right after it; the other variants' come last, so
    # they never push a synonym past the variant cap
    typed = possessive_of(variants[0])
    out = variants[:1] + ([typed] if typed and typed not in variants else []) + variants[1:]
    for variant in variants[1:]:
        possessive = possessive_of(variant)
        if possessive and possessive not in out:
            out.append(possessive)
    return out

_possessive_cache = dict[tuple[str, str, str], bool]()

_typed_possessive = re.compile(r"[A-Za-z]{3,}['\u2019]s\b")

def _typed_without_possessive(subterm: str, normalized: str) -> str | None:
    """'Crohn's disease' (normalized 'crohn s disease') -> 'crohn disease'. Only when typed with an
    apostrophe: an 's' on its own is often part of a name ('hemoglobin S', 'protein S deficiency')."""
    return _without_possessive(normalized) if _typed_possessive.search(subterm) else None

def _without_possessive(phrase: str) -> str | None:
    words = phrase.split(" ")
    kept = [w for i, w in enumerate(words) if not (w == "s" and i > 0 and words[i - 1].isalpha() and len(words[i - 1]) >= 3)]
    return " ".join(kept) if len(kept) < len(words) else None

@functools.lru_cache(maxsize=100_000)
def get_variants(subterm: str, data_dir: str) -> list[str]:
    """Return the subterm's variants, with the subterm itself first."""
    normalized = util.sanitize_term_for_search(subterm, keep_logical_operators=False)
    if not normalized:
        return []

    # the index is lowercase, so acronyms collide with ordinary words ('MS' -> 'ms', 'ADD' -> 'add').
    # acronyms are therefore never inflected, and only searched for when the user wrote them.
    # an acronym the user wrote is only looked up where it is an acronym too, so the gene
    # 'IDS' doesn't pick up the synonyms of the word 'ids' (plural of the psychoanalytic 'id').
    # a word typed in capitals that is only known as a word is treated as one ('ABACAVIR', from
    # all-caps lists), but not one that's also an acronym ('IDS', 'AVEN', 'MS').
    table = load_synonyms(data_dir)
    acronyms = table.acronyms
    keys = [normalized] + _inflections(normalized)
    typed_acronym = _is_acronym(subterm.split()[-1]) and (
        normalized in acronyms or not any(key in table.word_synonyms for key in keys))

    # MeSH synonyms (look up the singular form too, e.g. 'breast tumors' -> 'breast tumor')
    variants = [normalized]
    if typed_acronym:
        acronyms = acronyms | {normalized}
        lookup = [(table.acronym_synonyms, normalized)]
    else:
        if without_possessive := _typed_without_possessive(subterm, normalized):
            keys.append(without_possessive)  # 'Crohn's disease' -> MeSH 'crohn disease'
            variants.append(without_possessive)
        lookup = [(table.word_synonyms, key) for key in keys]
        if not any(key in table.word_synonyms for key in keys):
            lookup.append((table.acronym_synonyms, normalized))  # e.g. 'aids' written in lowercase

    for synonyms, key in lookup:
        for synonym in synonyms.get(key, []):
            if synonym not in variants and synonym not in acronyms:
                variants.append(synonym)

    # leave out a word that papers mostly use as an abbreviation for something else: 'Prisma'
    # (a brand of mesoglycan) is nearly always PRISMA, the systematic review guideline. names with
    # digits ('IL8', 'CD31') are specific, and are often defined in other words ('interleukin 8')
    variants = variants[:1] + [v for v in variants[1:] if not v.isalpha() or v not in table.abbreviations_used_for
                               or set(table.abbreviations_used_for[v]) & set(variants)]

    # abbreviations defined in papers, only when written as papers write them (see the module docstring)
    abbreviation = table.abbreviation_synonyms.get(normalized)
    if abbreviation and _case_key(subterm) == abbreviation[0]:
        acronyms = acronyms | {normalized}
        variants.extend(long for long in abbreviation[1] if long not in variants)

    # plural/singular and hyphenation of each variant
    expanded = []
    for variant in variants:
        expanded.append(variant)
        others = _hyphenations(variant)
        if variant not in acronyms:
            others += _inflections(variant)
        expanded.extend(other for other in others if other not in variants and other not in expanded)

    return [v for v in expanded if util.get_ngram_n(v) <= MAX_NGRAM_N]

_letters_then_digits = re.compile(r"^([a-z]{2,})(\d+)$")
_letters_space_digits = re.compile(r"^([a-z]{2,}) (\d+)$")
# words often followed by a number in running text ('the ratio is 2', 'at 3 months', 'stage 4');
# 'is2' or 'stage4' as a name doesn't mean 'is 2' or 'stage 4' there
_WORDS_BEFORE_NUMBERS = FUNCTION_WORDS | {
    "is", "are", "was", "were", "be", "been", "no", "not", "than", "vs", "about", "over", "under",
    "day", "days", "week", "weeks", "month", "months", "year", "years", "age", "aged", "hour", "hours",
    "grade", "stage", "phase", "step", "group", "groups", "level", "dose", "doses", "visit", "cycle", "arm",
}

def _hyphenations(phrase: str) -> list[str]:
    """Hyphens become spaces in the index, so 'IL-6' is stored as 'il 6' and doesn't match 'il6'.
    Give the other spelling of names like 'il6' / 'il 6', 'covid19' / 'covid 19'."""
    match = _letters_then_digits.match(phrase) or _letters_space_digits.match(phrase)
    if not match or match[1] in _WORDS_BEFORE_NUMBERS:
        return []
    return [f"{match[1]}{match[2]}" if " " in phrase else f"{match[1]} {match[2]}"]

def _inflections(phrase: str) -> list[str]:
    """Plural/singular forms of a phrase, varying only its last word."""
    words = phrase.split(" ")
    last = words[-1]
    if len(last) < 4 or not last.isalpha() or FUNCTION_WORDS.intersection(words):
        return []

    singular = _inflect_engine.singular_noun(last)
    if last.endswith(("is", "us", "ss")):
        singular = False  # these are already singular; inflect would give e.g. 'sclerosis' -> 'sclerosi'
    if last.endswith("ics"):
        return []  # mostly fields of study, whose 'singular' is an adjective ('genetics' -> 'genetic')
    other = singular if singular else _inflect_engine.plural_noun(last)
    if not other or other == last:
        return []
    other = util.sanitize_term_for_search(other, keep_logical_operators=False)
    return [" ".join(words[:-1] + [other])]

def _case_key(term: str) -> str:
    # 'HSV-2' and 'HSV 2' are written the same way; 'Mg' and 'MG' aren't
    return re.sub(r"[^A-Za-z0-9]", "", term)

def _normalize(term: str) -> str:
    return util.sanitize_term_for_search(term, keep_logical_operators=False)

def _is_acronym(word: str) -> bool:
    # only all-letter acronyms can collide with ordinary words; codes like 'LY2835219' can't
    return word.isalpha() and sum(c.isupper() for c in word) >= 2

@dataclasses.dataclass
class SynonymTable:
    word_synonyms: dict[str, list[str]]     # a term's synonyms in the groups where it's written as a word
    acronym_synonyms: dict[str, list[str]]  # ... and in the groups where it's written as an acronym
    acronyms: set[str]                      # terms written as an acronym in any group
    # mined short form -> (how papers write it, see _case_key; its long forms)
    abbreviation_synonyms: dict[str, tuple[str, list[str]]] = dataclasses.field(default_factory=dict)
    # every mined short form -> its long forms, including ones MeSH/HGNC also define
    abbreviations_used_for: dict[str, list[str]] = dataclasses.field(default_factory=dict)

_synonym_cache = dict[str, SynonymTable]()
_cache_state = dict[str, tuple[tuple, float]]()  # data dir -> (table files' mtimes, time loaded)
_last_checked = dict[str, float]()                # data dir -> time the table files were last checked

def _table_mtimes(data_dir: str) -> tuple:
    paths = [os.path.join(data_dir, name) for name in (SYNONYMS_FILENAME, ABBREVIATIONS_FILENAME)]
    return tuple(os.path.getmtime(p) if os.path.exists(p) else None for p in paths)

def _drop_stale_cache(data_dir: str) -> None:
    """Workers live a long time, so reload when the table files change (abbreviations are
    rebuilt after indexing), and check daily whether a new MeSH year is out. Files are checked
    at most every CHECK_FILES_EVERY_SECONDS, since this runs on every search and the data dir
    may be on a network filesystem."""
    if data_dir not in _synonym_cache:
        return
    now = time.time()
    if now - _last_checked.get(data_dir, 0.0) < CHECK_FILES_EVERY_SECONDS:
        return
    _last_checked[data_dir] = now
    mtimes, loaded_at = _cache_state.get(data_dir, (None, 0.0))
    if mtimes != _table_mtimes(data_dir) or now - loaded_at > RELOAD_AFTER_SECONDS:
        del _synonym_cache[data_dir]
        get_variants.cache_clear()
        _verb_cache.clear()
        _possessive_cache.clear()

def load_synonyms(data_dir: str) -> SynonymTable:
    """Map each term to all of its synonyms, and find the terms that end in an acronym.
    Downloads and builds the table if it is missing, from an older MeSH year, or in an old format.
    Abbreviations mined from the corpus are added if they have been built."""
    if data_dir in _synonym_cache:
        return _synonym_cache[data_dir]

    stored = refresh_synonym_table(data_dir)
    table = SynonymTable(dict(), dict(), set())
    _add_groups(table, stored["groups"], stored["acronyms"])
    gene_symbols = set(stored["gene_symbols"])

    # MeSH and HGNC are curated, so mined abbreviations only fill in short forms they don't define,
    # and never give a gene symbol another meaning: 'AES' (now TLE5) isn't 'adverse events' (from 'AEs')
    for short, *longs in read_abbreviation_table(data_dir) or []:
        key = _normalize(short)
        table.abbreviations_used_for[key] = longs
        if key in table.word_synonyms or key in table.acronym_synonyms or key in gene_symbols:
            continue
        table.abbreviation_synonyms[key] = (_case_key(short), longs)
        if len(longs) > 1:
            _add_groups(table, [longs], [[]])  # spellings of the long form ('human papilloma virus')

    _synonym_cache[data_dir] = table
    _cache_state[data_dir] = (_table_mtimes(data_dir), time.time())
    return table

def refresh_synonym_table(data_dir: str) -> dict:
    """Build the table if it is missing or in an old format, or try to rebuild it if it is from
    an older MeSH year, and return its contents. Called by the indexing job, so searches rarely
    have to wait for the download, and by the first search that needs the table."""
    path = os.path.join(data_dir, SYNONYMS_FILENAME)
    stored = _read_table(path)
    if not _needs_refresh(stored, path):
        return stored

    # one worker downloads and builds; the others wait for it, then use its table
    os.makedirs(data_dir, exist_ok=True)
    with open(os.path.join(data_dir, SYNONYMS_FILENAME + ".lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        stored = _read_table(path)
        if not _needs_refresh(stored, path):
            return stored
        try:
            build_synonym_table(data_dir)
            stored = _read_table(path)
        except FastKmException as e:
            if stored is None:
                raise
            os.utime(path)  # don't retry until RETRY_REFRESH_AFTER_SECONDS have passed
            print(f"Could not refresh synonyms, using the {stored.get('year')} table: {e}")
    return stored

def _read_table(path: str) -> dict | None:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        stored = json.load(f)
    return stored if stored.get("format") == TABLE_FORMAT else None

def _needs_refresh(stored: dict | None, path: str) -> bool:
    # MeSH for year Y is released late in year Y-1, so a table older than this year is out of date.
    # a table that couldn't be refreshed, or could only be built from an older MeSH year because
    # this year's isn't out yet, is tried again after a while rather than on every search.
    if stored is None:
        return True
    outdated = stored.get("year", 0) < time.localtime().tm_year
    return outdated and time.time() - os.path.getmtime(path) > RETRY_REFRESH_AFTER_SECONDS

def _add_groups(table: SynonymTable, groups: list[list[str]], acronyms: list[list[str]]) -> None:
    for group, group_acronyms in zip(groups, acronyms):
        for term in group:
            synonyms = table.acronym_synonyms if term in group_acronyms else table.word_synonyms
            existing = synonyms.setdefault(term, [])
            existing.extend(t for t in group if t != term and t not in existing)
        table.acronyms.update(group_acronyms)

def build_synonym_table(data_dir: str, year: int | None = None) -> None:
    """Download the MeSH descriptor and supplementary concept files and the HGNC gene table,
    and save their synonym groups to the data dir."""
    this_year = time.localtime().tm_year
    years = [year] if year else [this_year + 1, this_year, this_year - 1]
    os.makedirs(data_dir, exist_ok=True)

    try:
        print(f"Downloading {HGNC_URL}")
        with urllib.request.urlopen(HGNC_URL, timeout=300) as response:
            hgnc_tsv = response.read().decode("utf-8")
        gene_groups, gene_symbols = _parse_hgnc_tsv(hgnc_tsv), _hgnc_symbols(hgnc_tsv)
    except Exception as e:
        raise FastKmException(f"Could not download HGNC gene names for synonym expansion: {e}")

    last_error = None
    for y in years:
        try:
            groups = []
            for url in MESH_URLS:
                groups.extend(_download_and_parse(url.format(year=y), data_dir))
            break
        except Exception as e:
            last_error = e
            print(f"Could not use MeSH {y}: {e}")
    else:
        raise FastKmException(f"Could not download MeSH synonyms for synonym expansion: {last_error}")

    save_synonym_table(data_dir, y, groups + gene_groups, gene_symbols=gene_symbols)
    print(f"Saved {len(groups)} MeSH {y} and {len(gene_groups)} HGNC synonym groups")

def save_synonym_table(data_dir: str, year: int, groups: list[list[str]], gene_symbols: list[str] = ()) -> None:
    """Normalize synonym groups (terms in their original case) the way queries are, and save them."""
    normalized_groups, group_acronyms = [], []
    for group in groups:
        normalized_group, acronyms = [], []
        for term in group:
            normalized = util.sanitize_term_for_search(term, keep_logical_operators=False)
            if normalized and normalized not in normalized_group and util.get_ngram_n(normalized) <= MAX_NGRAM_N:
                normalized_group.append(normalized)
                if _is_acronym(term.split()[-1]):
                    acronyms.append(normalized)
        if len(normalized_group) > 1:
            normalized_groups.append(normalized_group)
            group_acronyms.append(acronyms)

    _write_json(data_dir, SYNONYMS_FILENAME, {"format": TABLE_FORMAT, "year": year, "groups": normalized_groups,
                                              "acronyms": group_acronyms, "gene_symbols": sorted({_normalize(s) for s in gene_symbols})})

def save_abbreviation_table(data_dir: str, groups: list[list[str]]) -> None:
    """Save mined abbreviations, each [short form as papers write it, long forms...], with long forms
    normalized the way queries are."""
    entries = []
    for short, *longs in groups:
        key = _normalize(short)
        normalized = []
        for long in longs:
            n = _normalize(long)
            if n and n != key and n not in normalized and util.get_ngram_n(n) <= MAX_NGRAM_N:
                normalized.append(n)
        if key and normalized:
            entries.append([short] + normalized)
    _write_json(data_dir, ABBREVIATIONS_FILENAME, {"format": ABBREVIATIONS_FORMAT, "abbreviations": entries})

def read_abbreviation_table(data_dir: str) -> list[list[str]] | None:
    """The saved abbreviations, or None if they haven't been built or are in an old format."""
    path = os.path.join(data_dir, ABBREVIATIONS_FILENAME)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        stored = json.load(f)
    return stored["abbreviations"] if stored.get("format") == ABBREVIATIONS_FORMAT else None

def _write_json(data_dir: str, filename: str, data: dict) -> None:
    fd, tmp_path = tempfile.mkstemp(dir=data_dir, suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f)
    os.replace(tmp_path, os.path.join(data_dir, filename))  # atomic, in case several workers build at once

def _download_and_parse(url: str, data_dir: str) -> list[list[str]]:
    # the supplementary file is ~800 MB uncompressed, so stream it from disk rather than memory
    print(f"Downloading {url}")
    fd, tmp_path = tempfile.mkstemp(dir=data_dir, suffix=".gz.tmp")
    try:
        with os.fdopen(fd, "wb") as f, urllib.request.urlopen(url, timeout=300) as response:
            while chunk := response.read(1 << 20):
                f.write(chunk)
        with gzip.open(tmp_path, "rb") as xml_file:
            return _parse_mesh_xml(xml_file)
    finally:
        os.remove(tmp_path)

def _parse_mesh_xml(xml_file) -> list[list[str]]:
    """Extract synonym groups, in their original case: one per descriptor concept,
    and one per supplementary record."""
    groups = []
    record_group = None # collects a supplementary record's concepts; None within descriptors
    for event, elem in ET.iterparse(xml_file, events=("start", "end")):
        if event == "start":
            if elem.tag == "SupplementalRecord":
                record_group = []
            continue

        if elem.tag == "Concept":
            concept_group = []
            for string in elem.iterfind("TermList/Term/String"):
                # skip inverted forms like 'Neoplasms, Breast' and qualified ones like
                # 'MS (Multiple Sclerosis)'; they never occur in running text
                if "," in string.text or "(" in string.text:
                    continue
                if string.text not in concept_group:
                    concept_group.append(string.text)

            if record_group is None:
                groups.append(concept_group)
            else:
                record_group.extend(t for t in concept_group if t not in record_group)
            elem.clear()
        elif elem.tag == "SupplementalRecord":
            groups.append(record_group)
            record_group = None
            elem.clear()
        elif elem.tag == "DescriptorRecord":
            elem.clear()  # free each record once parsed

    return [g for g in groups if len(g) > 1]

def _parse_hgnc_tsv(tsv: str) -> list[list[str]]:
    """Extract one synonym group per approved human gene, in original case."""
    genes = [row for row in csv.DictReader(tsv.splitlines(), delimiter="\t") if row["status"] == "Approved"]

    # previous symbols and aliases are only used when they name a single gene.
    # spacing is ignored, so 'COX-2' and 'COX2' count as the same name.
    n_genes_named = dict[str, int]()
    for gene in genes:
        names = {gene["symbol"]} | _split_hgnc_list(gene["prev_symbol"]) | _split_hgnc_list(gene["alias_symbol"])
        for name in {_gene_name_key(n) for n in names}:
            n_genes_named[name] = n_genes_named.get(name, 0) + 1

    groups = []
    for gene in genes:
        group = [gene["symbol"], gene["name"]]
        for name in sorted(_split_hgnc_list(gene["prev_symbol"])) + sorted(_split_hgnc_list(gene["alias_symbol"])):
            normalized = util.sanitize_term_for_search(name, keep_logical_operators=False)
            if n_genes_named[_gene_name_key(name)] == 1 and _looks_like_gene_id(normalized) and name not in group:
                group.append(name)
        groups.append(group)
    return groups

def _hgnc_symbols(tsv: str) -> list[str]:
    """Every current and previous approved gene symbol, including ones too ambiguous to use as synonyms."""
    symbols = set()
    for row in csv.DictReader(tsv.splitlines(), delimiter="\t"):
        if row["status"] == "Approved":
            symbols |= {row["symbol"]} | _split_hgnc_list(row["prev_symbol"])
    return sorted(symbols)

def _gene_name_key(name: str) -> str:
    return util.sanitize_term_for_search(name, keep_logical_operators=False).replace(" ", "")

def _looks_like_gene_id(alias: str) -> bool:
    """True for aliases like 'p53', 'cpp32', 'il 8'; false for ones that read as ordinary
    text once lowercased, like 'ray', 'men', 'and 1' or '6 3'."""
    return (len(alias) >= MIN_GENE_ALIAS_LENGTH
            and any(c.isdigit() for c in alias)
            and any(c.isalpha() for c in alias)
            and not FUNCTION_WORDS.intersection(alias.split(" ")))

def _split_hgnc_list(field: str) -> set[str]:
    return {name for name in field.strip('"').split("|") if name}
