import mmap
import pickle
import math
import os
import gc
import time
import pymongo
from pymongo import errors
import indexing.km_util as util
from indexing.abstract_catalog import AbstractCatalog

delim = '\t'
mongo_cache = None

class Index():
    def __init__(self, pubmed_abstract_dir: str):
        # caches
        self._query_cache = dict()
        self._token_cache = dict()
        self._n_articles_by_pub_year = dict()
        _connect_to_mongo()

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

    def query_index(self, query: str) -> 'set[int]':
        query = query.lower().strip()

        if query in self._query_cache:
            return self._query_cache[query]
        else:
            cached_result = _check_mongo_for_query(query)
            if not isinstance(cached_result, type(None)):
                return cached_result

        tokens = util.get_tokens(query)

        if len(tokens) > 100:
            raise ValueError("Query must have <=100 words")
        if not tokens:
            return set()

        start_time = time.perf_counter()
        result = self._query_disk(tokens)
        run_time = time.perf_counter() - start_time

        if run_time > 0.1 and len(result) < 1000:
            _place_in_mongo(query, result)

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

    def _query_disk(self, tokens: 'list[str]') -> 'set[int]':
        result = set()

        possible_pmids = set()
        for i, token in enumerate(tokens):
            # deserialize the tokens
            if token not in self._token_cache:
                self._token_cache[token] = self._read_token_from_disk(token)

        # find the set of PMIDs that contain all of the tokens
        # (not necessarily in order)
        possible_pmids = _intersect_dict_keys([self._token_cache[token] for token in tokens])

        # handle 1-grams
        if len(tokens) == 1:
            return possible_pmids

        # handle >1-grams
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

    def _read_token_from_disk(self, token: str) -> dict:
        if token not in self._byte_offsets:
            self._token_cache[token] = dict()
        elif token not in self._token_cache:
            stored_bytes = self._read_bytes_from_disk(token)

            # disabling garbage collection speeds up the 
            # deserialization process by 2-3x
            gc.disable()
            deserialized_dict = pickle.loads(stored_bytes)
            gc.enable()

            self._token_cache[token] = deserialized_dict

        return self._token_cache[token]

    def _read_bytes_from_disk(self, token: str) -> bytes:
        byte_info = self._byte_offsets[token]
        byte_offset = byte_info[0]
        byte_len = byte_info[1]

        self.connection.seek(byte_offset)
        stored_bytes = self.connection.read(byte_len)
        return stored_bytes

def _intersect_dict_keys(dicts: 'list[dict]'):
    lowest_n_keys = sorted(dicts, key=lambda x: len(x))[0]
    key_intersect = set(lowest_n_keys.keys())

    if len(dicts) == 1:
        return key_intersect

    for key in lowest_n_keys:
        for item in dicts:
            if key not in item:
                key_intersect.remove(key)
                break

    return key_intersect

def _connect_to_mongo():
    global mongo_cache
    try:
        loc = 'mongo'
        client = pymongo.MongoClient(loc, 27017)
        db = client["query_cache_db"]
        mongo_cache = db["query_cache"]
        mongo_cache.create_index('query', unique=True) #expireafterseconds=72 * 60 * 60, 
    except:
        print('Warning: could not find a MongoDB instance to use as a query cache')
        pass

def _check_mongo_for_query(query: str):
    if not isinstance(mongo_cache, type(None)):
        print('fetching ' + query + ' from mongo cache')

        result = mongo_cache.find_one({'query': query})

        if not isinstance(result, type(None)):
            print('fetched ' + query + ' from mongo cache')
            return set(result['result'])
        else:
            return None
    else:
        print('no mongo cache to fetch from')
        return None

def _place_in_mongo(query, result):
    if not isinstance(mongo_cache, type(None)):
        print('posting ' + query + ' to mongo cache')

        try:
            mongo_cache.insert_one({'query': query, 'result': list(result)})
            print('posted ' + query + ' to mongo cache')
        except errors.DuplicateKeyError:
            # tried to insert and got a duplicate key error. probably just the result
            # of a race condition (another worker added the query record).
            # it's fine, just continue on.
            pass
    else:
        print('no mongo cache to post to')