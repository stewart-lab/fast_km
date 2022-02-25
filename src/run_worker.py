import multiprocessing
import os
import time
import argparse
from workers.km_worker import start_worker
import indexing.download_abstracts as downloader
from indexing.index_builder import IndexBuilder
import workers.loaded_index as li
import indexing.km_util as util

parser = argparse.ArgumentParser()
parser.add_argument('-w', '--workers', default=1)
parser.add_argument('-b', '--build_index', default=False)
args = parser.parse_args()

def start_workers(do_multiprocessing = True):
    n_workers = args.workers

    if type(n_workers) is str:
        n_workers = int(n_workers)

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
    build_index = rebuild_index or (not os.path.exists(util.get_index_file(li.pubmed_path)))

    if build_index:
        #downloader.bulk_download()
        index_builder = IndexBuilder(li.pubmed_path)
        index_builder.build_index()

    start_workers()

if __name__ == '__main__':
    main()