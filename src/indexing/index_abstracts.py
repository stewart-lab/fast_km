import os
import glob
from typing import Iterable
import indexing.km_util as util
from indexing.indexer import Indexer

def get_index_dir(abstracts_dir: str) -> str:
    return os.path.join(abstracts_dir, 'Index')

def get_db_path(abstracts_dir: str) -> str:
    return os.path.join(get_index_dir(abstracts_dir), 'db.db')

def get_files_to_index(abstracts_dir: str, already_indexed: Iterable) -> 'list[str]':
    all_abstracts_files = glob.glob(os.path.join(abstracts_dir, "*.xml.gz"))
    not_indexed_yet = []

    for file in all_abstracts_files:
        if os.path.basename(file) not in already_indexed:
            not_indexed_yet.append(file)

    return not_indexed_yet

# TODO: unzip .gz.md5
# TODO: verify .xml with .md5
def index_abstracts(abstracts_dir: str, n_per_cache_dump = 10) -> Indexer:
    """"""
    print('Building index...')

    the_indexer = Indexer(get_db_path(abstracts_dir))
    already_indexed_files = the_indexer.list_indexed_files()
    files_to_index = get_files_to_index(abstracts_dir, already_indexed_files)

    util.report_progress(0, len(files_to_index))

    for i, gzip_file in enumerate(files_to_index):
        the_indexer.index_xml_file(gzip_file)
        i = i + 1
        util.report_progress(i, len(files_to_index))

        if i % n_per_cache_dump == 0:
            the_indexer.dump_index_to_trie()

            # DEBUG
            #break

    the_indexer.finish_building_index()
    print('Done building index')
    return the_indexer