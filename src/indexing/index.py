import quickle
import math
import os
import gc
import json
import pymongo
import cdblib
import sys
from pymongo import errors
import indexing.km_util as util
from indexing.abstract_catalog import AbstractCatalog

delim = '\t'
logical_or = '|' # supports '|' to mean 'or'
logical_and = '&' # supports '&' to mean 'and'
mongo_cache = None
bytes_deserialized_counter = 0

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
        self._abstract_catalog = util.get_abstract_catalog(pubmed_abstract_dir)
        self._publication_years = dict()
        self._citation_count = dict()
        self._load_citation_data()
        self._date_censored_pmids = dict()
        self._open_mmap_connection()
        self.n_articles() # precalculate total N articles
        self._ngram_n = self._get_ngram_n()

        if self.connection:
            self.connection.close()
            self._open_mmap_connection()
            
        self.ngram_cache = dict()

    def construct_abstract_set(self, term: str) -> set:
        # TODO: support parenthesis for allowing OR and AND at the same time?
        # e.g., "(cancer/carcinoma) & BRCA1"

        term = sanitize_term(term)
        is_cached, pmid_set = self.check_caches_for_term(term)

        if is_cached:
            return pmid_set

        if logical_or in term:
            terms = get_subterms(term)
            pmid_set = set()
            for synonym in terms:
                pmid_set.update(self._query_index(synonym))
        elif logical_and in term:
            terms = get_subterms(term)
            pmid_set = set(self._query_index(terms[0]))
            for t in terms[1:]:
                pmid_set.intersection_update(self._query_index(t))
        else:
            pmid_set = self._query_index(term)

        if len(pmid_set) < 10000:
            _place_in_mongo(term, pmid_set)
        self._query_cache[term] = pmid_set

        return pmid_set

    def censor_by_year(self, pmids: 'set[int]', censor_year_lower: int, censor_year_upper: int, term: str) -> 'set[int]':
        """Censor PMIDs by year range (lower <= year <= upper)."""
        # year <0 and >2100 are excluded to prevent abuse
        if censor_year_lower < 0:
            censor_year_lower = 0
        if censor_year_upper < 0:
            return set()
        cache_key = (censor_year_lower, censor_year_upper)

        if cache_key not in self._date_censored_pmids:
            if not self._publication_years:
                self._init_pub_years()

            censored_set = set()

            for pmid, year in self._publication_years.items():
                if censor_year_lower <= year <= censor_year_upper:
                    censored_set.add(pmid)
            self._date_censored_pmids[cache_key] = censored_set

        if (term, cache_key) in self._date_censored_query_cache:
            return self._date_censored_query_cache[(term, cache_key)]
        
        date_censored_pmid_set = self._date_censored_pmids[cache_key] & pmids
        self._date_censored_query_cache[(term, cache_key)] = date_censored_pmid_set

        self._publication_years = dict()
        return date_censored_pmid_set
    
    def top_n_by_citation_count(self, pmids: 'set[int]', top_n_articles = math.inf) -> 'list[int]':
        if top_n_articles == math.inf:
            return list(pmids)

        if not self._citation_count:
            return list(pmids)[:top_n_articles]
        
        # sort by citation count (descending order) and return top N
        # TODO: avoid casting the PMIDs as strings, probably adds a fair bit of time
        top_n_sorted = sorted(pmids, key=lambda pmid: -self._citation_count.get(str(pmid), 0))[:top_n_articles]
        return top_n_sorted
    
    def top_n_by_pmid(self, pmids: 'set[int]', top_n_articles = math.inf) -> 'list[int]':
        if top_n_articles == math.inf:
            return list(pmids)

        # sort by PMID (descending order) and return top N
        top_n_sorted = sorted(pmids, reverse=True)[:top_n_articles]
        return top_n_sorted

    def n_articles(self, censor_year_lower: int = 0, censor_year_upper: int = math.inf) -> int:
        """Returns the number of indexed abstracts within a year range."""
        # year <0 and >2100 are excluded to prevent abuse
        if censor_year_lower < 0:
            censor_year_lower = 0
        if censor_year_upper < 0:
            return 0
        
        cache_key = (censor_year_lower, censor_year_upper)

        if cache_key in self._n_articles_by_pub_year:
            return self._n_articles_by_pub_year[cache_key]
        
        if not self._publication_years:
            self._init_pub_years()

        n_articles_censored = 0
        for pmid, year in self._publication_years.items():
            if censor_year_lower <= year <= censor_year_upper:
                n_articles_censored += 1
                
        self._n_articles_by_pub_year[cache_key] = n_articles_censored

        self._publication_years = dict()
        return n_articles_censored

    def decache_token(self, token: str):
        ltoken = sanitize_term(token)
        if ltoken in self._token_cache:
            del self._token_cache[ltoken]
        if ltoken in self._query_cache:
            del self._query_cache[ltoken]
        if ltoken in self._date_censored_query_cache:
            del self._date_censored_query_cache[ltoken]

    def check_caches_for_term(self, term: str):
        if term in self._query_cache:
            # check RAM cache
            return (True, self._query_cache[term])
        elif term in self._token_cache:
            self._query_cache[term] = set(self._token_cache[term].keys())
            return (True, self._query_cache[term])
        else:
            # check mongoDB cache
            result = _check_mongo_for_query(term)
            if not isinstance(result, type(None)):
                self._query_cache[term] = result
                return (True, result)

        return (False, None)

    def get_ngrams(self, tokens: 'list[str]') -> 'list[str]':
        if self._ngram_n > 1 and len(tokens) > 1:
            ngrams = []
            for i in range(0, len(tokens) - (self._ngram_n - 1)):
                ngram = str.join(' ', tokens[i:i + self._ngram_n])
                ngrams.append(ngram)
            return ngrams
        else:
            return tokens

    def get_highest_priority_term(self, list_of_terms: 'list[str]', token_dict: dict):
        if self._token_cache:
            highest_priority = -1
            highest_priority_term = list_of_terms[0]

            for token in self._token_cache:
                if token in token_dict:
                    terms_for_token = token_dict[token]

                    for term in terms_for_token:
                        priority = self._get_term_priority(term)

                        if priority > highest_priority:
                            highest_priority = priority
                            highest_priority_term = term
        else:
            highest_priority_term = list_of_terms[0]

        return highest_priority_term
        
    def _load_citation_data(self) -> None:
        try:
            with open(util.get_icite_file(self._pubmed_dir), encoding="utf-8") as f:
                self._citation_count = json.load(f)
        except:
            print("WARNING: could not citation count data. jobs will still complete but PMIDs will not be in citation count order.")

    def _get_term_priority(self, term: str):
        if term in self.ngram_cache:
            ngrams = self.ngram_cache[term]
        else:
            subterms = get_subterms(term)
            all_ngrams = []
            for subterm in subterms:
                tokens = util.get_tokens(subterm)
                ngrams = self.get_ngrams(tokens)
                all_ngrams.extend(ngrams)
            self.ngram_cache[term] = all_ngrams
            ngrams = all_ngrams

        priority = 0
        n_cached_tokens = 0

        for ngram in ngrams:
            if ngram in self._token_cache:
                priority += len(self._token_cache[ngram])
                n_cached_tokens += 1

        if n_cached_tokens == len(ngrams):
            if priority < 100000:
                priority = sys.maxsize - 1
            else:
                priority = sys.maxsize

        return priority

    def _query_index(self, query: str) -> 'set[int]':
        query = util.sanitize_text(query)

        is_cached, result = self.check_caches_for_term(query)
        if is_cached:
            return result

        tokens = util.get_tokens(query)

        if len(tokens) > 100:
            print("ERROR: Query failed, must have <=100 words; query was " + query)
            return set()
            # raise ValueError("Query must have <=100 words")
        if not tokens:
            return set()

        result = self._query_disk(tokens)

        if len(result) < 10000 or len(tokens) > 1:
            _place_in_mongo(query, result)

        self._query_cache[query] = result

        return result

    def _open_mmap_connection(self) -> None:
        if not os.path.exists(self._bin_path):
            print('WARNING: index does not exist and needs to be built. queries will return empty values until index is built.')
            self.connection = None
            return

        self.connection = cdblib.Reader64.from_file_path(self._bin_path)

    def _init_pub_years(self) -> None:
        if self.connection:
            pub_bytes = self._read_bytes_from_disk('ABSTRACT_PUBLICATION_YEARS')
        else:
            return

        if pub_bytes:
            self._publication_years = quickle.loads(pub_bytes)

        if not self._publication_years:
            catalog = AbstractCatalog(self._pubmed_dir)
            cat_path = util.get_abstract_catalog(self._pubmed_dir)
            for abs in catalog.stream_existing_catalog(cat_path):
                self._publication_years[abs.pmid] = abs.pub_year

    def _query_disk(self, tokens: 'list[str]') -> 'set[int]':
        result = set()

        tokens = self.get_ngrams(tokens)

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
        stored_bytes = self._read_bytes_from_disk(token)
        if not stored_bytes:
            self._token_cache[token] = dict()
        else:
            # disabling garbage collection speeds up the 
            # deserialization process by 2-3x
            gc.disable()
            deserialized_dict = quickle.loads(stored_bytes)
            gc.enable()

            self._token_cache[token] = deserialized_dict

        return self._token_cache[token]

    def _read_bytes_from_disk(self, token: str) -> bytes:
        if not self.connection:
            return None

        token_bytes = self.connection.get(token)

        if token_bytes:
            global bytes_deserialized_counter
            bytes_deserialized_counter += len(token_bytes)

        if bytes_deserialized_counter > 100000000:
            self.connection.close()
            self._open_mmap_connection()
            bytes_deserialized_counter = 0

        return token_bytes

    def _get_ngram_n(self) -> int:
        n = 1

        if not self.connection:
            return n

        for i, key in enumerate(self.connection.iterkeys()):
            spl = str(key).split(' ')
            n = max(n, len(spl))
            if i > 100:
                break
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

def get_subterms(term: str) -> 'list[str]':
    if logical_or in term:
        terms = term.split(logical_or)
        return terms
    elif logical_and in term:
        terms = term.split(logical_and)
        return terms
    else:
        return [term]

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
        host = util.mongo_address.split(':')[0]
        port = int(util.mongo_address.split(':')[1])
        client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS = 500, connectTimeoutMS = 500)
        db = client["query_cache_db"]
        mongo_cache = db["query_cache"]
        mongo_cache.create_index('query', unique=True)
    except:
        print('WARNING: could not find a MongoDB instance to use as a query cache. jobs will complete but may be slower than normal.')
        mongo_cache = None

def _check_mongo_for_query(query: str) -> bool:
    if not isinstance(mongo_cache, type(None)):
        try:
            result = mongo_cache.find_one({'query': query})
        except:
            print('WARNING: non-fatal error in retrieving from mongo. job may complete slower than normal.')
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
            print('WARNING: non-fatal AutoReconnect error in inserting to mongo. job may complete slower than normal.')
            pass
        except errors.DocumentTooLarge:
            pass
    else:
        pass

def _empty_mongo() -> None:
    if not isinstance(mongo_cache, type(None)):
        x = mongo_cache.delete_many({})
        print('INFO: mongodb cache cleared, ' + str(x.deleted_count) + ' items were deleted')