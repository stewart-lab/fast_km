import nltk
from src.documents.document import Document

word_tokenizer = nltk.RegexpTokenizer(r"\w+") # remove all non-word characters
word_tokenizer_lop = nltk.RegexpTokenizer(r"\w+|[&|()]") # keep logical operators as separate tokens
logical_operators = ['&', '|', '(', ')']

def sanitize_term_for_search(term: str, keep_logical_operators=True) -> str:
    tokens = _tokenize_text(term, keep_logical_operators)

    if not keep_logical_operators:
        sanitized = ' '.join(tokens)
    else:
        out = []
        for i, token in enumerate(tokens):
            if i > 0 and token not in logical_operators and tokens[i-1] not in logical_operators:
                out.append(' ')
            out.append(token)
        sanitized = ''.join(out)
    return sanitized

def get_subterms(composite_term: str, keep_logical_operators=False) -> list[str]:
    """Split a composite term into subterms based on logical operators."""
    tokens = _tokenize_text(composite_term, True)
    subterms = []
    current_subterm = []
    for token in tokens:
        if token in logical_operators:
            subterms.append(' '.join(current_subterm).strip())
            current_subterm = []
            if keep_logical_operators:
                subterms.append(token)
        else:
            current_subterm.append(token)
    if current_subterm:
        subterms.append(' '.join(current_subterm).strip())
    subterms = [subterm for subterm in subterms if subterm]  # remove empty strings
    return subterms

def get_ngrams(subterm: str, n: list[int] = [1, 2]) -> list[str]:
    """Get n-grams from a list of tokens."""
    tokens = _tokenize_text(subterm, keep_logical_operators=False)
    ngrams = []
    for i in range(len(tokens)):
        for k in n:
            if i + k <= len(tokens):
                ngram = str.join(' ', tokens[i:i+k])
                ngrams.append(ngram)
    return ngrams

def get_ngram_n(ngram: str) -> int:
    return ngram.count(' ') + 1

def index_document(doc: Document, index: dict) -> None:
    # get unigrams and bigrams
    n = 2
    
    # tokenize the title
    tokens = _tokenize_text(doc.title, keep_logical_operators=False)
    for i, token in enumerate(tokens):
        for k in range(i + 1, min(len(tokens) + 1, i + n + 1)):
            ngram = str.join(' ', tokens[i:k])
            loc = i
            _place_ngram(ngram, loc, doc.pmid, index)

    # initialize 'i' for abstracts without a title
    if not tokens:
        i = 0

    # tokenize the abstract text
    tokens = _tokenize_text(doc.abstract, keep_logical_operators=False)
    for j, token in enumerate(tokens):
        for k in range(j + 1, min(len(tokens) + 1, j + n + 1)):
            ngram = str.join(' ', tokens[j:k])
            loc = i + j + 2
            _place_ngram(ngram, loc, doc.pmid, index)

def _tokenize_text(text: str, keep_logical_operators: bool) -> list[str]:
    """Split text into a list of words (tokens)."""
    # underscore counts as a 'word character' in regex but not in our search system.
    # we replace underscore with a space here, which is a non-word character in regex.
    text = text.lower().strip().replace('_', ' ')
    if not keep_logical_operators:
        tokens = word_tokenizer.tokenize(text)
    else:
        tokens = word_tokenizer_lop.tokenize(text)

    return tokens

def _place_ngram(ngram: str, pos: int, pmid: int, index: dict) -> None:
    # for unigrams we presumably do not need their position information
    # so we store those as a set of PMIDs instead of a dict of PMIDs to positions
    is_unigram = ' ' not in ngram
    if is_unigram:
        pmids = index.get(ngram, set())
        pmids.add(pmid)
        index[ngram] = pmids
        return

    pmid_to_position = index.get(ngram, dict())
    if pmid not in pmid_to_position:
        pmid_to_position[pmid] = []
    pmid_to_position[pmid].append(pos)
    index[ngram] = pmid_to_position
