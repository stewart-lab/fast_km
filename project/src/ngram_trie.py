import pygtrie
import pickle
from io import BytesIO

class NGramTrie():
    def __init__(self):
        self.pub_years = dict()
        self.indexed_abstracts = set()
        self.trie = pygtrie.StringTrie()

    def ngrams(self):
        return self.trie.keys()

    def get_indexed_abstracts_files(self):
        return self.indexed_abstracts

    def get_pub_years(self):
        return self.pub_years
    
    def query(self, query: str) -> 'set[int]':
        if query in self.trie:
            return pickle.loads(self.trie[query])
        return set()

    def pickle_cache(self, ngrams_with_pmids: dict, pub_years: dict, indexed_filenames: list) -> None:
        # save the index cache to the db
        self._pickle_ngrams(ngrams_with_pmids)
        self._save_pub_years(pub_years)
        self._save_indexed_filenames(indexed_filenames)

    def combine_pickled_sets(self):
        for ngram in self.trie:
            p = self.trie[ngram]
            combined_set = set()
            s = BytesIO(p)

            while True:
                try:
                    partial_set = pickle.load(s)
                    combined_set = combined_set | partial_set
                except EOFError:
                    pass
                    break

            combined_set_pickled = pickle.dumps(combined_set)
            self.trie[ngram] = combined_set_pickled

    def _pickle_ngrams(self, ngrams_to_pmids: dict):
        for ngram in ngrams_to_pmids:
            pickled = pickle.dumps(ngrams_to_pmids[ngram])

            if ngram in self.trie:
                self.trie[ngram] = self.trie[ngram] + pickled
            else:
                self.trie[ngram] = pickled

    def _save_pub_years(self, pub_years) -> None:
        for pmid in pub_years:
            self.pub_years[pmid] = pub_years[pmid]

    def _save_indexed_filenames(self, filenames) -> None:
        for file in filenames:
            self.indexed_abstracts.add(file)