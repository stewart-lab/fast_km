"""
Abbreviations mined from the corpus itself.

Papers define their abbreviations in running text, e.g. 'human papillomavirus (HPV)'.
These definitions are found with the Schwartz & Hearst algorithm (A simple algorithm for
identifying abbreviation definitions in biomedical text, PSB 2003), counted over the
whole corpus, and an abbreviation is kept only when:
  - it is defined often enough to trust (MIN_DEFINITIONS), and
  - it nearly always means the same thing (MIN_SHARE): 'HPV' is almost always 'human
    papillomavirus', but 'MS' is split between 'multiple sclerosis' and 'mass spectrometry'.
Spellings of the same long form ('human papilloma virus', 'human papillomaviruses',
'acquired immune deficiency syndrome') are counted together, and each one common enough
is kept as a synonym. Short forms are compared in lowercase, as the index stores them.

The kept abbreviations are saved as synonym groups, next to the MeSH/HGNC table, and are
updated whenever new documents are indexed.
"""

import collections
import concurrent.futures
import difflib
import multiprocessing
import os
import re
import sqlite3
import tempfile
import msgpack
import src.indexing.indexing_util as util
import src.indexing.synonyms as synonyms

ABBREVIATIONS_FILENAME = synonyms.ABBREVIATIONS_FILENAME
COUNTS_DIRNAME = "abbreviation_counts"  # definition counts per document table

MIN_DEFINITIONS = 20         # times an abbreviation must be defined to be trusted
MIN_SHARE = 0.8              # share of its definitions its most common meaning must have
MIN_SPELLING_SHARE = 0.02    # share of those definitions a spelling of that meaning needs to be kept
MAX_SPELLINGS = 10
SAME_MEANING_SIMILARITY = 0.8  # how alike two long forms' letters must be to count as one meaning
SAME_WORD_SIMILARITY = 0.85    # ... and how alike each pair of words that differ ('immune deficiency' vs 'immunodeficiency')

# 'long form (SF)': the short form is 2-10 characters, at most two words, and starts with a letter or digit
_definition = re.compile(r"\(([A-Za-z0-9][^()\[\];,=<>%]{1,9})\)")
_clause_break = re.compile(r"[.;:!?\[\]()]")

def find_definitions(text: str) -> list[tuple[str, str]]:
    """Find (short form, long form) pairs like ('HPV', 'human papillomavirus') in text."""
    pairs = []
    for match in _definition.finditer(text):
        short = match.group(1).strip()
        if not _is_valid_short_form(short):
            continue

        # candidate long form: the words before '(', within the same clause, at most min(|SF| + 5, 2|SF|) words
        before = text[:match.start()]
        clause_start = max((m.end() for m in _clause_break.finditer(before)), default=0)
        words = before[clause_start:].split()
        n_chars = len(short.replace(" ", "").replace("-", ""))
        words = words[-min(n_chars + 5, n_chars * 2):]
        if not words:
            continue

        long = _best_long_form(short, " ".join(words))
        if long and _is_valid_pair(short, long):
            pairs.append((short, long))
    return pairs

def _is_valid_short_form(short: str) -> bool:
    # 'HPV', 'IL-6', 'mAb'; not ordinary lowercase words like '(see below)'
    return len(short) >= 2 and len(short.split()) <= 2 and any(c.isupper() for c in short)

def _best_long_form(short: str, candidate: str) -> str | None:
    """Schwartz & Hearst: match the short form's characters right to left in the candidate;
    its first character must start a word."""
    s, l = len(short) - 1, len(candidate) - 1
    while s >= 0:
        c = short[s].lower()
        if not c.isalnum():
            s -= 1
            continue
        while l >= 0 and (candidate[l].lower() != c or (s == 0 and l > 0 and candidate[l - 1].isalnum())):
            l -= 1
        if l < 0:
            return None
        l -= 1
        s -= 1
    return candidate[candidate.rfind(" ", 0, l + 1) + 1:]

def _is_valid_pair(short: str, long: str) -> bool:
    long_words = long.split()
    return (len(long) > len(short)
            and short.lower() not in (w.lower() for w in long_words)
            and len(long_words) <= 10)

def count_definitions(texts) -> collections.Counter:
    """Count (short form, normalized long form) definitions over texts."""
    counts = collections.Counter()
    for text in texts:
        for short, long in find_definitions(text):
            normalized = util.sanitize_term_for_search(long, keep_logical_operators=False)
            if normalized:
                counts[(short, normalized)] += 1
    return counts

def select_groups(counts: collections.Counter) -> list[list[str]]:
    """Keep the abbreviations that are defined often and nearly always mean the same thing.
    Returns synonym groups [short form, long form spellings...]."""
    # short forms are counted as the index stores them, so 'PSA' and 'PsA', or 'HSV-2' and
    # 'HSV 2', are one search term and are counted together
    search_terms = {short: _search_term(short) for short in {short for short, _long in counts}}
    totals = collections.Counter()
    for (short, _long), n in counts.items():
        totals[search_terms[short]] += n

    # most short forms are defined only a few times; skip them before grouping spellings
    casings = collections.defaultdict(collections.Counter)
    meanings = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))
    for (short, long), n in counts.items():
        term = search_terms[short]
        if term and totals[term] >= MIN_DEFINITIONS:
            casings[term][short] += n
            meanings[term][_meaning_key(long)][long] += n

    groups = []
    for short, by_key in meanings.items():
        # the most common meaning, with every spelling close enough to it to be the same
        # ('acquired immune deficiency syndrome', 'herpes simplex virus 2' vs '... type 2')
        key_counts = {key: sum(spellings.values()) for key, spellings in by_key.items()}
        top_key = max(key_counts, key=key_counts.get)
        top_long = by_key[top_key].most_common(1)[0][0]
        top = collections.Counter()
        for key, spellings in by_key.items():
            if key == top_key or _same_meaning(top_long, spellings.most_common(1)[0][0]):
                top.update(spellings)

        n_top = sum(top.values())
        if n_top / totals[short] < MIN_SHARE:
            continue
        spellings = [long for long, n in top.most_common(MAX_SPELLINGS) if n / n_top >= MIN_SPELLING_SHARE]
        groups.append([casings[short].most_common(1)[0][0]] + spellings)
    return groups

def _search_term(short: str) -> str:
    return util.sanitize_term_for_search(short, keep_logical_operators=False)

_roman_numeral = re.compile(r"^(ii|iii|iv|vi|vii|viii|ix|xi|xii)$")
_filler_words = {"a", "an", "the", "of", "in", "for", "with", "to", "and", "type"}

def _same_meaning(long: str, other: str) -> bool:
    """Two long forms are spellings of one meaning if they differ only in spacing, plurals, small
    spelling changes, or filler words ('acquired immune deficiency syndrome', 'herpes simplex virus
    2' vs '... type 2'). A different word makes a different meaning, even if its letters are close
    ('chronic liver disease' vs 'chronic lung disease'), and so does a different member of a series
    ('angiotensin I' vs 'angiotensin II', 'protein kinase A' vs 'protein kinase C')."""
    if _series_markers(long) != _series_markers(other):
        return False
    matcher = difflib.SequenceMatcher(None, _meaning_key(long), _meaning_key(other), autojunk=False)
    if matcher.real_quick_ratio() < SAME_MEANING_SIMILARITY or matcher.ratio() < SAME_MEANING_SIMILARITY:
        return False

    # compare word by word: each differing stretch must be a respelling of the same words
    words, other_words = long.split(), other.split()
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, words, other_words, autojunk=False).get_opcodes():
        if tag == "equal":
            continue
        if i1 == i2 or j1 == j2:  # words only one of them has
            if not set(words[i1:i2] + other_words[j1:j2]) <= _filler_words:
                return False
            continue
        a, b = _meaning_key(" ".join(words[i1:i2])), _meaning_key(" ".join(other_words[j1:j2]))
        if a != b and difflib.SequenceMatcher(None, a, b, autojunk=False).ratio() < SAME_WORD_SIMILARITY:
            return False
    return True

def _series_markers(long: str) -> list[str]:
    return sorted(w for w in long.split() if w.isdigit() or len(w) == 1 or _roman_numeral.match(w))

def _meaning_key(long: str) -> str:
    """Spellings of the same long form share a key: 'human papilloma virus', 'human papillomaviruses'.
    A rough plural rule is enough here, and is much faster than inflect over millions of long forms."""
    key = long.replace(" ", "")
    if key.endswith("ies"):
        return key[:-3] + "y"
    if key.endswith(("ses", "xes", "ches", "shes")):
        return key[:-2]
    if key.endswith("s") and not key.endswith(("ss", "us", "is")):
        return key[:-1]
    return key

def update_abbreviation_table(data_dir: str, changed_tables: list[str], n_workers: int = 8) -> None:
    """Re-mine the document tables that changed, and save the kept abbreviations to the data dir.
    Counts are kept per table, so after the first build only tables with new documents are re-read."""
    counts_dir = os.path.join(data_dir, COUNTS_DIRNAME)
    os.makedirs(counts_dir, exist_ok=True)
    corpus_path = os.path.join(data_dir, "_corpus.db")

    corpus = sqlite3.connect(f"file:{corpus_path}?mode=ro", uri=True)
    tables = [row[0] for row in corpus.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'documents_%'")]
    corpus.close()
    to_mine = [t for t in tables if t in changed_tables or not os.path.exists(_counts_path(counts_dir, t))]
    if not to_mine and synonyms.read_abbreviation_table(data_dir) is not None:
        return  # up to date

    if len(to_mine) == 1:
        _mine_table(corpus_path, to_mine[0], counts_dir)
    elif to_mine:
        print(f"Mining abbreviations from {len(to_mine)} document tables...")
        with concurrent.futures.ProcessPoolExecutor(min(n_workers, len(to_mine)), mp_context=multiprocessing.get_context("spawn")) as pool:
            list(pool.map(_mine_table, [corpus_path] * len(to_mine), to_mine, [counts_dir] * len(to_mine)))

    counts = collections.Counter()
    for table in tables:
        with open(_counts_path(counts_dir, table), "rb") as f:
            counts.update({(short, long): n for short, long, n in msgpack.unpack(f)})

    groups = select_groups(counts)
    synonyms.save_abbreviation_table(data_dir, groups)
    print(f"Saved {len(groups)} abbreviations mined from the corpus")

def _mine_table(corpus_path: str, table: str, counts_dir: str) -> None:
    corpus = sqlite3.connect(f"file:{corpus_path}?mode=ro", uri=True)
    # titles rarely end in punctuation; end them, so a long form can't start in the title
    texts = (f"{title or ''}. {abstract or ''}" for title, abstract in corpus.execute(f"SELECT title, abstract FROM {table}"))
    counts = count_definitions(texts)
    corpus.close()

    fd, tmp_path = tempfile.mkstemp(dir=counts_dir, suffix=".tmp")
    with os.fdopen(fd, "wb") as f:
        msgpack.pack([[short, long, n] for (short, long), n in counts.items()], f)
    os.replace(tmp_path, _counts_path(counts_dir, table))

def _counts_path(counts_dir: str, table: str) -> str:
    return os.path.join(counts_dir, f"{table}.msgpack")
