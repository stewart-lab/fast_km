import os
import quickle
import cdblib
from io import BytesIO
import indexing.km_util as util
from indexing.abstract import Abstract
from indexing.abstract_catalog import AbstractCatalog

class IndexBuilder():
    def __init__(self, path_to_pubmed_abstracts: str):
        self.path_to_pubmed_abstracts = path_to_pubmed_abstracts
        self.abstract_years = dict()

    def build_index(self, dump_rate = 300000, overwrite_old = True):
        print('INFO: cataloging abstracts...')
        # catalog abstracts
        abstract_catalog = AbstractCatalog(self.path_to_pubmed_abstracts)
        abstract_catalog.catalog_abstracts()
        abstract_catalog.catalog.clear() # saves RAM

        print('INFO: building index...')

        # initialize the abstract year dict with large number of keys (much faster to add to)
        self.abstract_years = dict.fromkeys(range(len(abstract_catalog.abstract_files) * 50000))
        
        # build the index
        catalog_path = util.get_abstract_catalog(self.path_to_pubmed_abstracts)
        cold_storage = dict()
        hot_storage = dict()
        
        for i, abstract in enumerate(abstract_catalog.stream_existing_catalog(catalog_path)):
            self._index_abstract(abstract, hot_storage)
            self.abstract_years[abstract.pmid] = abstract.pub_year

            if i % dump_rate == 0:
                self._serialize_hot_to_cold_storage(hot_storage, cold_storage)

                # sort of difficult to report progress because we don't know
                # the total number of abstracts
                print('INFO: done with ' + str(i + 1) + ' abstracts')

        # write the index
        self.abstract_years = {pmid:year for pmid, year in self.abstract_years.items() if year}
        self._serialize_hot_to_cold_storage(hot_storage, cold_storage, consolidate_cold_storage=True)
        self._write_index_to_disk(cold_storage, overwrite_old)

    def overwrite_old_index(self):
        temp_index_path = util.get_index_file(self.path_to_pubmed_abstracts) + '.tmp'

        os.replace(temp_index_path, util.get_index_file(self.path_to_pubmed_abstracts))

    def _index_abstract(self, abstract: Abstract, hot_storage: dict, n = 2):
        tokens = util.get_tokens(abstract.title)
        for i, token in enumerate(tokens):
            for k in range(i + 1, min(len(tokens) + 1, i + n + 1)):
                ngram = str.join(' ', tokens[i:k])
                self._place_token(ngram, i, abstract.pmid, hot_storage)

        if not tokens:
            i = 0

        tokens = util.get_tokens(abstract.text)
        for j, token in enumerate(tokens):
            for k in range(j + 1, min(len(tokens) + 1, j + n + 1)):
                ngram = str.join(' ', tokens[j:k])
                self._place_token(ngram, i + j + 2, abstract.pmid, hot_storage)

    def _place_token(self, token: str, pos: int, id: int, hot_storage: dict) -> None:
        l_token = token.lower()

        if l_token not in hot_storage:
            hot_storage[l_token] = dict()

        tokens = hot_storage[l_token]

        if id not in tokens:
            tokens[id] = pos
        elif type(tokens[id]) is int:
            tokens[id] = [tokens[id], pos]
        else: # type is list
            tokens[id].append(pos)

    def _serialize_hot_to_cold_storage(self, hot_storage: dict, cold_storage: dict, consolidate_cold_storage = False):
        # this is sketchy... if the quickle protocol changes its delimiter
        # then the following line will need to be changed to build the index
        byte_delim = b'././.'

        # append serialized hot storage to cold storage
        for token in hot_storage:
            serialized = quickle.dumps(hot_storage[token])
            
            if token in cold_storage:
                cold_storage[token] = cold_storage[token] + byte_delim + serialized
            else:
                cold_storage[token] = serialized

        hot_storage.clear()

        # merge the appended dictionaries if desired
        if consolidate_cold_storage:
            for token in cold_storage:
                appended_serialized_dictionaries = cold_storage[token]
                combined_dict = dict()
                byte_stream = BytesIO(appended_serialized_dictionaries)

                partial_serialized = byte_stream.read()
                while len(partial_serialized) > 0:
                    try:
                        partial = quickle.loads(partial_serialized)
                        combined_dict.update(partial)
                    except:
                        print('ERROR: problem combining dictionaries for: ' + token)

                    try:
                        byte_len = partial_serialized.index(byte_delim)
                    except ValueError:
                        pass
                        break

                    partial_serialized = partial_serialized[byte_len + len(byte_delim):]
                    
                serialized_combined_dict = quickle.dumps(combined_dict)
                cold_storage[token] = serialized_combined_dict

    def _write_index_to_disk(self, cold_storage: dict, overwrite_old = True):
        dir = os.path.dirname(util.get_index_file(self.path_to_pubmed_abstracts))
        
        if not os.path.exists(dir):
            os.mkdir(dir)

        temp_index_path = util.get_index_file(self.path_to_pubmed_abstracts) + '.tmp'

        with open(temp_index_path, 'wb') as f:
            with cdblib.Writer64(f) as writer:
                writer.put('ABSTRACT_PUBLICATION_YEARS', quickle.dumps(self.abstract_years))

                for token in cold_storage:
                    serialized_pmids = cold_storage[token]
                    writer.put(token, serialized_pmids)

        # done writing; rename the temp files
        if overwrite_old:
            self.overwrite_old_index()