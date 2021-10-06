import math
import os
import pickle
from project.src.database import Database

class Index():
    def __init__(self, path_to_db: str):
        self._ngrams_with_pmids = dict()
        self._publication_years = dict()
        self._indexed_filenames = set()
        self.path_to_db = path_to_db

        if os.path.exists(path_to_db):
            self._db = self._load_existing_trie()
        else:
            self._db = Database()

    def dump_cache_to_db(self):
        """Call this method while building the index periodically. Saves 
        info from in-memory dictionaries into the database and clears 
        in-memory dictionaries"""
        self._db.pickle_cache(self._ngrams_with_pmids, self._publication_years, self._indexed_filenames)
        self._ngrams_with_pmids = dict()
        self._publication_years = dict()
        self._save_to_disk()

    def start_building_index(self):
        self._ngrams_with_pmids = dict()
        self._publication_years = dict()

    def finish_building_index(self):
        self.dump_cache_to_db()
        self._db.combine_pickled_sets()
        self.dump_cache_to_db()
        
    def ngrams(self):
        return self._db.ngrams()
    
    def query_index(self, query: str) -> 'set[int]':
        """Returns a set of PMIDs that contain the query n-gram"""
        query_lower = query.lower()
        
        if query_lower in self._ngrams_with_pmids:
            return self._ngrams_with_pmids[query_lower]
        else:
            return self._db.query(query_lower)

    def get_publication_year(self, id: int) -> int:
        if id in self._publication_years:
            return self._publication_years[id]
        else:
            return self._db.pub_years[id]

    def place_value(self, search_term: str, id: int, pub_year: int) -> None:
        lower_word = search_term.lower()

        if lower_word not in self._ngrams_with_pmids:
            self._ngrams_with_pmids[lower_word] = set()

        self._ngrams_with_pmids[lower_word].add(id)
        self._publication_years[id] = pub_year
    
    def n_articles(self, censor_year = math.inf) -> int:
        if censor_year == math.inf:
            return len(self._db.pub_years)
        else:
            num_articles_censored = 0

            for pmid in self._db.pub_years:
                if self._db.pub_years[pmid] <= censor_year:
                    num_articles_censored += 1
        
            return num_articles_censored

    def populate_fet_table(self, a_term_set: set, b_term_set: set, censor_year: int):
        """Populates the table for the Fisher's exact test"""
        a_term_and_b_term = len(a_term_set & b_term_set)
        not_a_term_b_term = len(b_term_set) - a_term_and_b_term
        a_term_not_b_term = len(a_term_set) - a_term_and_b_term
        not_a_term_not_b_term = self.n_articles(censor_year) - a_term_and_b_term - not_a_term_b_term - a_term_not_b_term

        table = [[a_term_and_b_term, not_a_term_b_term],
                [a_term_not_b_term, not_a_term_not_b_term]]

        return table

    def get_indexed_abstracts_files(self) -> 'list[str]':
        return self._db.get_indexed_abstracts_files()

    def censor_by_year(self, original_set: set, censor_year: int) -> set:
        """"""
        new_set = set()

        for item in original_set:
            if self.get_publication_year(item) <= censor_year:
                    new_set.add(item)

        return new_set

    def _load_existing_trie(self):
        with open(self.path_to_db, 'rb') as handle:
            b = pickle.load(handle)
            return b
        
    def _save_to_disk(self):
        with open(self.path_to_db, 'wb') as handle:
            pickle.dump(self._db, handle)