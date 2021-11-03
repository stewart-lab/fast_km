import multiprocessing
import os
import time
import argparse
from workers.km_worker import start_worker
import indexing.download_abstracts as downloader
import indexing.index_abstracts as indexer
import workers.loaded_index as li
from indexing.indexer import Indexer
import indexing.disk_index as di

parser = argparse.ArgumentParser()
parser.add_argument('-w', '--workers', default=1)
parser.add_argument('-b', '--build_index', default=False)
args = parser.parse_args()

def start_workers(do_multiprocessing = True):
    n_workers = args.workers

    print('starting ' + str(n_workers) + ' workers...')

    if do_multiprocessing:
        jobs = []
        for i in range(0, n_workers):
            p = multiprocessing.Process(target=start_worker)
            jobs.append(p)
            p.start()
    else:
        start_worker()

def main():
    print('workers waiting 10 sec for redis to set up...')
    time.sleep(10)
    li.pubmed_path = '/mnt/pubmed'

    rebuild_index = args.build_index
    build_index = rebuild_index or (not os.path.exists(li.flat_binary_path())) or (not os.path.exists(li.flat_text_path()))

    if build_index:
        #downloader.bulk_download()
        the_indexer = indexer.index_abstracts(li.pubmed_path, 10)
        di.write_byte_info(li.flat_binary_path(), li.flat_text_path(), li.flat_pub_years_path(), the_indexer)

    start_workers()

if __name__ == '__main__':
    main()