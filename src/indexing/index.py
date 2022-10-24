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
logical_or = '/' # supports '/' to mean 'or'
logical_and = '&' # supports '&' to mean 'and'
mongo_cache = None

class Index():
    def __init__(self, pubmed_abstract_dir: str):
        # caches
        self._query_cache = dict()
        self._token_cache = dict()
        self._date_censored_query_cache = dict()
        self._n_articles_by_pub_year = dict()
        _connect_to_mongo()

        self._pubmed_dir = pubmed_abstract_dir
        self._bin_path = util.get_index_file(pubmed_abstract_dir)
        self._offsets_path = util.get_offset_file(pubmed_abstract_dir)
        self._abstract_catalog = util.get_abstract_catalog(pubmed_abstract_dir)
        self._byte_offsets = dict()
        self._publication_years = dict()
        self._date_censored_pmids = dict()
        self._init_byte_info()
        self._open_connection()
        self._ngram_n = self._get_ngram_n()

    def close_connection(self) -> None:
        self.connection.close()
        self.file_obj.close()

    def construct_abstract_set(self, term: str) -> set:
        # TODO: support parenthesis for allowing OR and AND at the same time?
        # e.g., "(cancer/carcinoma) & BRCA1"

        term = sanitize_term(term)
        is_cached, pmid_set = self.check_caches_for_term(term)

        if is_cached:
            return pmid_set

        if logical_or in term:
            terms = term.split(logical_or)
            pmid_set = set()
            for synonym in terms:
                pmid_set.update(self._query_index(synonym))
        elif logical_and in term:
            terms = term.split(logical_and)
            pmid_set = self._query_index(terms[0])
            for t in terms[1:]:
                pmid_set.intersection_update(self._query_index(t))
        else:
            pmid_set = self._query_index(term)

        if len(pmid_set) < 10000:
            _place_in_mongo(term, pmid_set)
        self._query_cache[term] = pmid_set

        return pmid_set

    def censor_by_year(self, pmids: 'set[int]', censor_year: int, term: str) -> 'set[int]':
        if censor_year not in self._date_censored_pmids:
            censored_set = set()

            for pmid, year in self._publication_years.items():
                if year <= censor_year:
                    censored_set.add(pmid)
            self._date_censored_pmids[censor_year] = censored_set

        if (term, censor_year) in self._date_censored_query_cache:
            return self._date_censored_query_cache[(term, censor_year)]
        
        date_censored_pmid_set = self._date_censored_pmids[censor_year] & pmids
        self._date_censored_query_cache[(term, censor_year)] = date_censored_pmid_set

        return date_censored_pmid_set

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

    def check_caches_for_term(self, term: str):
        if term in self._query_cache:
            # check RAM cache
            return (True, self._query_cache[term])
        else:
            # check mongoDB cache
            result = _check_mongo_for_query(term)
            if not isinstance(result, type(None)):
                self._query_cache[term] = result
                return (True, result)

        return (False, None)

    def _query_index(self, query: str) -> 'set[int]':
        query = util.sanitize_text(query)

        is_cached, result = self.check_caches_for_term(query)
        if is_cached:
            return result

        tokens = util.get_tokens(query)

        if len(tokens) > 100:
            print("Query failed, must have <=100 words; query was " + query)
            return set()
            # raise ValueError("Query must have <=100 words")
        if not tokens:
            return set()

        result = self._query_disk(tokens)

        if len(result) < 10000 or len(tokens) > 1:
            _place_in_mongo(query, result)

        self._query_cache[query] = result

        return result

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

        if self._ngram_n > 1 and len(tokens) > 1:
            ngrams = []
            for i in range(0, len(tokens) - (self._ngram_n - 1)):
                ngram = str.join(' ', tokens[i:i + self._ngram_n])
                ngrams.append(ngram)
            tokens = ngrams

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

    def _get_ngram_n(self) -> int:
        n = 1
        tmp_list = list(self._byte_offsets.keys())[:100]
        for item in tmp_list:
            spl = item.split(' ')
            n = max(n, len(spl))
        return n

    def _check_if_mongo_should_be_refreshed(self, terms_to_check: 'list[str]' = ['fever']):
        # the purpose of this function is to check a non-cached version of a token
        # and compare to a cached version of a token. if the two do not produce
        # the same result, then the cache is outdated and needs to be cleared.

        # the list of terms to check should contain words that are frequent enough
        # where we can catch if the cache needs to be updated, but not so frequent
        # that they add a lot of overhead to every job.

        for item in terms_to_check:
            query = item.lower().strip()
            mongo_result = _check_mongo_for_query(query)

            tokens = util.get_tokens(query)
            result = self._query_disk(tokens)

            if isinstance(mongo_result, type(None)):
                _place_in_mongo(query, result)
                continue

            if result != mongo_result:
                return True

        return False

def sanitize_term(term: str) -> str:
    if logical_or in term or logical_and in term:
        sanitized_subterms = []

        if logical_or in term:
            string_joiner = logical_or
        elif logical_and in term:
            string_joiner = logical_and

        for subterm in term.split(string_joiner):
            sanitized_subterms.append(util.sanitize_text(subterm))

        sanitized_subterms.sort()
        sanitized_term = str.join(string_joiner, sanitized_subterms)
    else:
        sanitized_term = util.sanitize_text(term)

    return sanitized_term

def _intersect_dict_keys(dicts: 'list[dict]') -> None:
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

def _connect_to_mongo() -> None:
    # TODO: set expiration time for cached items (72h, etc.?)
    # mongo_cache.create_index('query', unique=True) #expireafterseconds=72 * 60 * 60, 
    global mongo_cache
    try:
        loc = 'mongo'
        client = pymongo.MongoClient(loc, 27017, serverSelectionTimeoutMS = 500, connectTimeoutMS = 500)
        db = client["query_cache_db"]
        mongo_cache = db["query_cache"]
        mongo_cache.create_index('query', unique=True)
    except:
        print('warning: could not find a MongoDB instance to use as a query cache')
        mongo_cache = None

def _check_mongo_for_query(query: str) -> bool:
    if not isinstance(mongo_cache, type(None)):
        try:
            result = mongo_cache.find_one({'query': query})
        except:
            print('warning: non-fatal error in retrieving from mongo')
            return None

        if not isinstance(result, type(None)):
            return set(result['result'])
        else:
            return None
    else:
        return None

def _place_in_mongo(query: str, result: 'set[int]') -> None:
    if not isinstance(mongo_cache, type(None)):
        try:
            mongo_cache.insert_one({'query': query, 'result': list(result)})
        except errors.DuplicateKeyError:
            # tried to insert and got a duplicate key error. probably just the result
            # of a race condition (another worker added the query record).
            # it's fine, just continue on.
            pass
        except errors.AutoReconnect:
            # not sure what this error is. seems to throw occasionally. just ignore it.
            print('warning: non-fatal AutoReconnect error in inserting to mongo')
            pass
    else:
        pass

def _empty_mongo() -> None:
    if not isinstance(mongo_cache, type(None)):
        x = mongo_cache.delete_many({})
        print('mongodb cache cleared, ' + str(x.deleted_count) + ' items were deleted')