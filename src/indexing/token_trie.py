import pygtrie
import pickle
from io import BytesIO

class TokenTrie():
    def __init__(self):
        self.pub_years = dict()
        self.indexed_abstracts = set()
        self.trie = pygtrie.StringTrie()

    def tokens(self):
        return self.trie.keys()

    def get_indexed_abstracts_files(self):
        return self.indexed_abstracts

    def get_pub_years(self):
        return self.pub_years
    
    def query(self, query: str) -> dict:
        if query in self.trie:
            return pickle.loads(self.trie[query])
        return dict()

    def serialize_index(self, ngrams_with_pmids: dict, pub_years: dict, indexed_filenames: list) -> None:
        # save the index cache to the db
        self._pickle_tokens(ngrams_with_pmids)
        self._save_pub_years(pub_years)
        self._save_indexed_filenames(indexed_filenames)

    def combine_serialized_sets(self):
        for token in self.trie:
            p = self.trie[token]
            combined_set = dict()
            s = BytesIO(p)

            while True:
                try:
                    partial_set = pickle.load(s)
                    combined_set.update(partial_set)
                except EOFError:
                    pass
                    break

            combined_set_pickled = pickle.dumps(combined_set)
            self.trie[token] = combined_set_pickled

    def _pickle_tokens(self, tokens_to_pmids: dict):
        for token in tokens_to_pmids:
            pickled = pickle.dumps(tokens_to_pmids[token])

            if token in self.trie:
                self.trie[token] = self.trie[token] + pickled
            else:
                self.trie[token] = pickled

    def _save_pub_years(self, pub_years) -> None:
        for pmid in pub_years:
            self.pub_years[pmid] = pub_years[pmid]

    def _save_indexed_filenames(self, filenames) -> None:
        for file in filenames:
            self.indexed_abstracts.add(file)