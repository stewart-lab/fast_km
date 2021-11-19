import os
import quickle
from io import BytesIO
import indexing.km_util as util
from indexing.abstract import Abstract
from indexing.abstract_catalog import AbstractCatalog

delim = '\t'

class IndexBuilder():
    def __init__(self, path_to_pubmed_abstracts: str):
        self.path_to_pubmed_abstracts = path_to_pubmed_abstracts

    def build_index(self, dump_rate = 300000):
        # catalog abstracts
        abstract_catalog = AbstractCatalog(self.path_to_pubmed_abstracts)
        abstract_catalog.catalog_abstracts(dump_rate)
        abstract_catalog.catalog.clear() # saves RAM

        # delete the old index. MUST do this because if indexing is
        # interrupted, then we will have a new catalog of abstracts but an
        # old index, with no way of knowing if the index is done building
        old_index = util.get_index_file(self.path_to_pubmed_abstracts)
        old_offsets = util.get_offset_file(self.path_to_pubmed_abstracts)
        if os.path.exists(old_index):
            os.remove(old_index)
        if os.path.exists(old_offsets):
            os.remove(old_offsets)

        # build the index
        catalog_path = util.get_abstract_catalog(self.path_to_pubmed_abstracts)
        cold_storage = dict()
        hot_storage = dict()
        for i, abstract in enumerate(abstract_catalog.stream_existing_catalog(catalog_path)):
            self._index_abstract(abstract, hot_storage)

            if i % dump_rate == 0:
                self._serialize_hot_to_cold_storage(hot_storage, cold_storage)
                self._write_index_to_disk(cold_storage)

                # sort of difficult to report progress because we don't know
                # the total number of abstracts
                print('done with ' + str(i + 1) + ' abstracts')

        # write the index
        self._serialize_hot_to_cold_storage(hot_storage, cold_storage, consolidate_cold_storage=True)
        self._write_index_to_disk(cold_storage)

    def _index_abstract(self, abstract: Abstract, hot_storage: dict):
        tokens = util.get_tokens(abstract.title)
        for i, token in enumerate(tokens):
            self._place_token(token, i, abstract.pmid, hot_storage)

        tokens = util.get_tokens(abstract.text)
        if str.isspace(abstract.title):
            i = 0
        for j, token in enumerate(tokens):
            self._place_token(token, i + j + 2, abstract.pmid, hot_storage)

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
        byte_delim = b'//'

        # append serialized hot storage to cold storage
        for token in hot_storage:
            serialized = quickle.dumps(hot_storage[token])
            
            if token in cold_storage:
                cold_storage[token] = cold_storage[token] + byte_delim + serialized
                yea = 0
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
                        print('problem combining dictionaries for: ' + token)

                    try:
                        byte_len = partial_serialized.index(byte_delim)
                    except ValueError:
                        pass
                        break

                    partial_serialized = partial_serialized[byte_len + len(byte_delim):]
                    
                serialized_combined_dict = quickle.dumps(combined_dict)
                cold_storage[token] = serialized_combined_dict

    def _write_index_to_disk(self, cold_storage: dict):
        dir = os.path.dirname(util.get_index_file(self.path_to_pubmed_abstracts))
        
        if not os.path.exists(dir):
            os.mkdir(dir)

        n_bytes = 0

        temp_index_path = util.get_index_file(self.path_to_pubmed_abstracts) + '.tmp'
        temp_offset_path = util.get_offset_file(self.path_to_pubmed_abstracts) + '.tmp'
        with open(temp_index_path, 'wb') as b:
            with open(temp_offset_path, 'w') as t:
                for token in cold_storage:
                    serialized_pmids = cold_storage[token]

                    t.write(token)
                    t.write(delim)
                    t.write(str(n_bytes))
                    t.write(delim)
                    t.write(str(len(serialized_pmids)))
                    n_bytes += len(serialized_pmids)
                    t.write('\n')

                    b.write(serialized_pmids)

        # done writing; rename the temp files
        os.rename(temp_index_path, util.get_index_file(self.path_to_pubmed_abstracts))
        os.rename(temp_offset_path, util.get_offset_file(self.path_to_pubmed_abstracts))