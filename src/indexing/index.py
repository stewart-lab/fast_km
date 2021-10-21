import math
import os
import pickle
#from multiprocessing import Lock
import indexing.km_util as util
from indexing.token_trie import TokenTrie
from indexing.abstract import Abstract

class Index():
    def __init__(self, path_to_db: str):
        self._query_cache = dict()
        self._tokens_with_pmids = dict()
        self._publication_years = dict()
        self._indexed_filenames = set()
        self.path_to_db = path_to_db
        #self.lock = Lock()

        if os.path.exists(path_to_db):
            self._trie = self._load_existing_trie()
        else:
            self._trie = TokenTrie()

    def dump_index_to_trie(self):
        """Call this method while building the index periodically. Saves 
        info from in-memory dictionaries into the database and clears 
        in-memory dictionaries"""
        self._trie.serialize_index(self._tokens_with_pmids, self._publication_years, self._indexed_filenames)
        self._tokens_with_pmids = dict()
        self._publication_years = dict()
        self._save_to_disk()

    def finish_building_index(self):
        self.dump_index_to_trie()
        self._trie.combine_serialized_sets()
        self.dump_index_to_trie()
        
    def tokens(self):
        """Returns the number of indexed tokens. Expensive operation."""
        # TODO: cache
        return self._trie.tokens()
    
    def query_index(self, query: str) -> 'set[int]':
        """Returns a set of PMIDs that contain the query term."""
        l_query = query.lower()

        if l_query in self._query_cache:
            return self._query_cache[l_query]
        else:
            # tokenize
            tokens = util.get_tokens(l_query)

            if len(tokens) > 10:
                raise ValueError("Query must have <=10 words")

            result = self._query_trie(tokens)
            self.save_to_cache(self._query_cache, l_query, result)
            return result

    def n_articles(self, censor_year = math.inf) -> int:
        """Returns the number of indexed abstracts. Expensive operation."""
        # TODO: cache
        if censor_year == math.inf:
            return len(self._trie.pub_years)
        else:
            num_articles_censored = 0

            for pmid in self._trie.pub_years:
                if self._trie.pub_years[pmid] <= censor_year:
                    num_articles_censored += 1
        
            return num_articles_censored

    def get_publication_year(self, id: int) -> int:
        if id in self._publication_years:
            return self._publication_years[id]
        else:
            return self._trie.pub_years[id]

    def place_token(self, token: str, pos: int, id: int, pub_year: int) -> None:
        l_token = token.lower()

        if l_token not in self._tokens_with_pmids:
            self._tokens_with_pmids[l_token] = dict()

        tokens = self._tokens_with_pmids[l_token]

        if id not in tokens:
            tokens[id] = pos
        elif type(tokens[id]) is int:
            tokens[id] = [tokens[id], pos]
        else: # type is list
            tokens[id].append(pos)

        self._publication_years[id] = pub_year
    
    def list_indexed_files(self) -> 'list[str]':
        return self._trie.get_indexed_abstracts_files()

    def censor_by_year(self, original_set: set, censor_year: int) -> set:
        """"""
        new_set = set()

        for pmid in original_set:
            if self.get_publication_year(pmid) <= censor_year:
                new_set.add(pmid)

        return new_set

    def index_abstract(self, abstract: Abstract):
        tokens = util.get_tokens(abstract.text)

        for i, token in enumerate(tokens):
            self.place_token(token, i, abstract.pmid, abstract.pub_year)

    def _load_existing_trie(self):
        with open(self.path_to_db, 'rb') as handle:
            b = pickle.load(handle)
            return b
        
    def _save_to_disk(self):
        dir = os.path.dirname(self.path_to_db)
        
        if not os.path.exists(dir):
            os.mkdir(dir)

        with open(self.path_to_db, 'wb') as handle:
            pickle.dump(self._trie, handle)

    def _query_trie(self, tokens: 'list[str]') -> 'set[int]':
        result = set()

        if tokens[0] in self._tokens_with_pmids:
            token0_pmids = self._tokens_with_pmids[tokens[0]]
        else:
            token0_pmids = self._trie.query(tokens[0])
            self.save_to_cache(self._tokens_with_pmids, tokens[0], token0_pmids)

        # handle 1-grams
        if len(tokens) == 1:
            for key in token0_pmids:
                result.add(key)
            return result

        # handle >1-grams
        for pmid in token0_pmids:
            possibly_in_pmid = True

            l = 0
            while True:
                if type(token0_pmids[pmid]) is list:
                    if l == len(token0_pmids[pmid]) - 1:
                        break

                    loc_0 = token0_pmids[pmid][l]
                else:
                    loc_0 = token0_pmids[pmid]
                    if l == 1:
                        break

                ngram_found_in_pmid = False

                for i, token in enumerate(tokens):
                    if i == 0: # or token == query_wildcard:
                        continue

                    if token in self._tokens_with_pmids:
                        token_pmids = self._tokens_with_pmids[token]
                    else:
                        token_pmids = self._trie.query(token)
                        self.save_to_cache(self._tokens_with_pmids, token, token_pmids)

                    if pmid not in token_pmids:
                        possibly_in_pmid = False
                        break

                    loc = loc_0 + i
                    if loc == token_pmids[pmid] or (type(token_pmids[pmid]) is list and loc in token_pmids[pmid]):
                        if i == len(tokens) - 1:
                            ngram_found_in_pmid = True
                    else:
                        break

                if not possibly_in_pmid or ngram_found_in_pmid:
                    break

                l += 1
            
            if ngram_found_in_pmid:
                result.add(pmid)

        return result

    def save_to_cache(self, cache, key, value):
        #with self.lock:
        cache[key] = value