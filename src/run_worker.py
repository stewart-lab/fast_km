import multiprocessing
from redis import Redis
from rq import Worker, Queue, Connection
import time
import os
import workers.km_worker as worker
import indexing.download_abstracts as downloader
import indexing.index_abstracts as indexer
import workers.loaded_index as li
import workers.shared_memory_access as sma
from workers.work import debug_mem_work

def populate_index():
    #li.pubmed_path = '/mnt/pubmed'
    li.pubmed_path = '/Users/rmillikin/PubmedAbstracts'

def start_workers():
    n_workers = 2
    jobs = []
    for i in range(0, n_workers):
        p = multiprocessing.Process(target=start_worker)
        jobs.append(p)
        p.start()

def start_worker():
    _r = Redis(host='redis', port=6379)
    _q = Queue(connection=_r)

    with Connection(connection=_r):
        w = worker.KmWorker(queues=_q)
        w.work()

def main():
    #populate_index()

    print("creating shared memory")
    li.shared_mem = worker._create_shared_memory()

    print("starting workers")
    start_workers()

    print("adding jobs")
    _r = Redis(host='redis', port=6379)
    _q = Queue(connection=_r)
    job1 = _q.enqueue(debug_mem_work)
    job2 = _q.enqueue(debug_mem_work)

    print(str(os.getpid()) + "; shared memory sleeping")
    time.sleep(20)

    worker.clean_up_memory(li.shared_mem)

if __name__ == '__main__':
    main()