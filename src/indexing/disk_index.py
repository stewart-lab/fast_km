import mmap
import pygtrie
import pickle
import math
import gc
import indexing.km_util as util
from indexing.indexer import Indexer

delim = '\t'

class DiskIndex():
    def __init__(self, bin_path: str, txt_path: str, pub_year_path: str):
        # caches
        self._query_cache = dict()
        self._token_cache = dict()
        self._n_articles_by_pub_year = dict()

        self._bin_path = bin_path
        self._txt_path = txt_path
        self._pub_year_path = pub_year_path
        self._offset_trie = pygtrie.StringTrie()
        self._publication_years = dict()
        self._init_byte_info()
        self._open_connection()

    def close_connection(self) -> None:
        self.connection.close()
        self.file_obj.close()

    def query_index(self, query: str) -> 'set[int]':
        query = query.lower()

        if query in self._query_cache:
            return self._query_cache[query]

        tokens = util.get_tokens(query)

        if len(tokens) > 10:
            raise ValueError("Query must have <=10 words")

        result = self._query_disk(tokens)
        self._query_cache[query] = result
        return result

    def censor_by_year(self, pmids: 'set[int]', censor_year: int) -> 'set[int]':
        censored_set = set()

        for pmid in pmids:
            if self._publication_years(pmid) <= censor_year:
                censored_set.add(pmid)

        return censored_set

    def n_articles(self, censor_year = math.inf) -> int:
        """Returns the number of indexed abstracts, given an optional 
        censor year."""
        if type(censor_year) is not int:
            raise TypeError('Censor year must be an integer')

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

    def _open_connection(self) -> None:
        self.file_obj = open(self._bin_path, mode='rb')
        self.connection = mmap.mmap(self.file_obj.fileno(), length=0, access=mmap.ACCESS_READ)

    def _init_byte_info(self) -> None:
        with open(self._txt_path, 'r') as t:
            for index, line in enumerate(t):
                split = line.split(sep=delim)
                key = split[0]
                byte_offset = int(split[1].strip())
                byte_len = int(split[2].strip())

                self._offset_trie[key] = (byte_offset, byte_len)

        with open(self._pub_year_path, 'r') as t:
            for index, line in enumerate(t):
                split = line.split(sep=delim)
                pmid = int(split[0].strip())
                year = int(split[1].strip())
                self._publication_years[pmid] = year

    def _query_disk(self, tokens: 'list[str]') -> 'set[int]':
        result = set()

        if tokens[0] in self._token_cache:
            token0_pmids = self._token_cache[tokens[0]]
        else:
            token0_pmids = self._read_token_from_disk(tokens[0])
            self._token_cache[tokens[0]] = token0_pmids

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

                    if token in self._token_cache:
                        token_pmids = self._token_cache[token]
                    else:
                        token_pmids = self._read_token_from_disk(token)
                        self._token_cache[token] = token_pmids

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

    def _read_token_from_disk(self, token: str) -> dict:
        if token not in self._offset_trie:
            self._token_cache[token] = dict()
        elif token not in self._token_cache:
            byte_info = self._offset_trie[token]
            byte_offset = byte_info[0]
            byte_len = byte_info[1]

            self.connection.seek(byte_offset)
            stored_bytes = self.connection.read(byte_len)

            deserialized_dict = pickle.loads(stored_bytes)
            self._token_cache[token] = deserialized_dict

        return self._token_cache[token]

def write_byte_info(path_bin: str, path_txt: str, pub_year_txt: str, the_index: Indexer):
    n_bytes = 0

    with open(path_bin, 'wb') as b:
        with open(path_txt, 'w') as t:
            for key in the_index._trie.trie.keys():
                value = the_index._trie.trie[key]

                t.write(key)
                t.write(delim)
                t.write(str(n_bytes))
                t.write(delim)
                t.write(str(len(value)))
                n_bytes += len(value)
                t.write('\n')

                b.write(value)

    with open(pub_year_txt, 'w') as t:
        for pmid in the_index._trie.pub_years:
            t.write(str(pmid))
            t.write(delim)
            t.write(str(the_index._trie.pub_years[pmid]))
            t.write('\n')