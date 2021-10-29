import multiprocessing
import os
from workers.km_worker import start_worker
import indexing.download_abstracts as downloader
import indexing.index_abstracts as indexer
import workers.loaded_index as li
from indexing.index import Index
import workers.disk_index as di

n_workers = 3
rebuild_index = False

def start_workers(do_multiprocessing = True):
    if do_multiprocessing:
        jobs = []
        for i in range(0, n_workers):
            p = multiprocessing.Process(target=start_worker)
            jobs.append(p)
            p.start()
    else:
        start_worker()

def main():
    build_index = rebuild_index or (not os.path.exists(li.flat_binary_path())) or (not os.path.exists(li.flat_text_path()))

    if build_index:
        #downloader.bulk_download()
        the_index = indexer.index_abstracts(li.pubmed_path)
        di.write_byte_info(li.flat_binary_path(), li.flat_text_path(), the_index._trie.trie)

    start_workers()

if __name__ == '__main__':
    main()