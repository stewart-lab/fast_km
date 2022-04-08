import mmap
import pickle
import math
import os
import gc
import indexing.km_util as util
from indexing.abstract_catalog import AbstractCatalog

delim = '\t'

class Index():
    def __init__(self, pubmed_abstract_dir: str):
        # caches
        self._query_cache = dict()
        self._token_cache = dict()
        self._n_articles_by_pub_year = dict()
        self._serialized_tokens_from_disk = dict()

        self._pubmed_dir = pubmed_abstract_dir
        self._bin_path = util.get_index_file(pubmed_abstract_dir)
        self._offsets_path = util.get_offset_file(pubmed_abstract_dir)
        self._abstract_catalog = util.get_abstract_catalog(pubmed_abstract_dir)
        self._byte_offsets = dict()
        self._publication_years = dict()
        self._init_byte_info()
        self._open_connection()

    def close_connection(self) -> None:
        self.connection.close()
        self.file_obj.close()

    def query_index(self, query: str, req_pmids: 'set[int]' = None) -> 'set[int]':
        query = query.lower().strip()

        if query in self._query_cache:
            return self._query_cache[query]

        tokens = util.get_tokens(query)

        if len(tokens) > 100:
            raise ValueError("Query must have <=100 words")
        if not tokens:
            return set()

        result = self._query_disk(tokens, req_pmids)
        self._query_cache[query] = result
        return result

    def censor_by_year(self, pmids: 'set[int]', censor_year: int) -> 'set[int]':
        censored_set = set()

        for pmid in pmids:
            if self._publication_years[pmid] <= censor_year:
                censored_set.add(pmid)

        return censored_set

    def n_articles(self, censor_year = math.inf) -> int:
        """Returns the number of indexed abstracts, given an optional 
        censor year."""
        # year <0 and >2100 are excluded to prevent abuse...
        if censor_year == math.inf or censor_year > 2100:
            return len(self._publication_years)
        elif censor_year < 0:
            return 0
        else:
            if censor_year in self._n_articles_by_pub_year:
                return self._n_articles_by_pub_year[censor_year]

            n_articles_censored = 0

            for pmid in self._publication_years:
                if self._publication_years[pmid] <= censor_year:
                    n_articles_censored += 1

            self._n_articles_by_pub_year[censor_year] = n_articles_censored
            return n_articles_censored

    def decache_token(self, token: str):
        ltoken = token.lower()
        if ltoken in self._token_cache:
            del self._token_cache[ltoken]
        if ltoken in self._query_cache:
            del self._query_cache[ltoken]

    def _open_connection(self) -> None:
        if not os.path.exists(self._bin_path):
            print('warning: index does not exist and needs to be built')
            return

        self.file_obj = open(self._bin_path, mode='rb')
        self.connection = mmap.mmap(self.file_obj.fileno(), length=0, access=mmap.ACCESS_READ)

    def _init_byte_info(self) -> None:
        if not os.path.exists(self._offsets_path):
            return

        with open(self._offsets_path, 'r', encoding=util.encoding) as t:
            for index, line in enumerate(t):
                split = line.split(sep=delim)
                key = split[0]
                byte_offset = int(split[1].strip())
                byte_len = int(split[2].strip())

                self._byte_offsets[key] = (byte_offset, byte_len)

        catalog = AbstractCatalog(self._pubmed_dir)
        cat_path = util.get_abstract_catalog(self._pubmed_dir)
        for abs in catalog.stream_existing_catalog(cat_path):
            self._publication_years[abs.pmid] = abs.pub_year

    def _query_disk(self, tokens: 'list[str]', req_pmids: 'set[int]' = None) -> 'set[int]':
        # handle 1-grams
        if len(tokens) == 1:
            token = tokens[0]
            if token not in self._token_cache:
                self._token_cache[token] = self._deserialize_token(token)
            return set(self._token_cache[token].keys())

        # handle >1-grams
        result = set()

        # get a small set of PMIDs to start with (we're looking for an intersect set)
        # the best start is the smallest already-deserialized PMID set.
        # if there are no deserialized sets, the best start is the smallest serialized PMID set.
        deserialized_sets = [self._token_cache[token] for token in tokens if token in self._token_cache]

        if req_pmids:
            deserialized_sets.append(req_pmids)

        if deserialized_sets:
            possible_pmids = _intersect_dict_keys(deserialized_sets)

            if not possible_pmids:
                return possible_pmids
        
        # some of the PMIDs are plausible based on the already-deserialized sets.
        # note that there may be no deserialized sets available at all, in which case
        # we have not eliminated any PMIDs from the possibilities.
        for token in tokens:
            if token not in self._token_cache and token not in self._serialized_tokens_from_disk:
                self._read_bytes_from_disk(token, persist_serialized=True)

        serialized_tokens_by_size = sorted([token for token in tokens if token in self._serialized_tokens_from_disk], key=lambda t: len(self._serialized_tokens_from_disk[t]))

        for i, token in enumerate(serialized_tokens_by_size):
            # deserialize the set
            token_pmids = self._deserialize_token(token)
            self._token_cache[token] = token_pmids

            if i == 0 and not deserialized_sets:
                possible_pmids = set(token_pmids.keys())
            else:
                possible_pmids &= set(token_pmids.keys())

            if not possible_pmids:
                return possible_pmids

        for pmid in possible_pmids:
            ngram_found_in_pmid = False
            token0_locations = self._token_cache[tokens[0]][pmid]

            if type(token0_locations) is int:
                token0_locations = [token0_locations]

            for start in token0_locations:
                for t, token in enumerate(tokens[1:], 1):
                    locations = self._token_cache[token][pmid]
                    expected_location = start + t

                    if (type(locations) is int and expected_location == locations) or (type(locations) is list and expected_location in locations):
                        if t == len(tokens) - 1:
                            ngram_found_in_pmid = True
                    else:
                        break

                if ngram_found_in_pmid:
                    break

            if ngram_found_in_pmid:
                result.add(pmid)

        return result

    def _deserialize_token(self, token: str) -> dict:
        if token not in self._byte_offsets:
            self._token_cache[token] = dict()
        elif token not in self._token_cache:
            stored_bytes = self._read_bytes_from_disk(token)

            if token in self._serialized_tokens_from_disk:
                del self._serialized_tokens_from_disk[token]

            # disabling garbage collection speeds up the 
            # deserialization process by 2-3x
            gc.disable()
            deserialized_dict = pickle.loads(stored_bytes)
            gc.enable()

            self._token_cache[token] = deserialized_dict

        return self._token_cache[token]

    def _read_bytes_from_disk(self, token: str, persist_serialized = False) -> bytes:
        if token in self._serialized_tokens_from_disk:
            return self._serialized_tokens_from_disk[token]

        byte_info = self._byte_offsets[token]
        byte_offset = byte_info[0]
        byte_len = byte_info[1]

        self.connection.seek(byte_offset)
        stored_bytes = self.connection.read(byte_len)

        if persist_serialized:
            self._serialized_tokens_from_disk[token] = stored_bytes

        return stored_bytes

    def _cache_tokens(self, terms: 'list[str]'):
        bytes_for_tokens = dict()

        for term in terms:
            tokens = util.get_tokens(term)

            for token in tokens:
                if token not in self._token_cache and token not in bytes_for_tokens:
                    if token in self._byte_offsets:
                        the_bytes = self._read_bytes_from_disk(token)
                        bytes_for_tokens[token] = the_bytes

        gc.disable()
        for token in bytes_for_tokens:
            stored_bytes = bytes_for_tokens[token]
            deserialized_dict = pickle.loads(stored_bytes)
            self._token_cache[token] = deserialized_dict
        gc.enable()

def _intersect_dict_keys(dicts: 'list[dict]'):
    lowest_n_keys = sorted(dicts, key=lambda x: len(x))[0]

    if isinstance(lowest_n_keys, set):
        key_intersect = lowest_n_keys
    else:
        key_intersect = set(lowest_n_keys.keys())

    if len(dicts) == 1:
        return key_intersect

    for key in lowest_n_keys:
        for item in dicts:
            if key not in item:
                key_intersect.remove(key)
                break

    return key_intersect