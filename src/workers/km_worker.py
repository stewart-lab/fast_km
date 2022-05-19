from redis import Redis
from rq import Worker, Queue, Connection
from indexing.index import Index
import workers.loaded_index as li
import time

class KmWorker(Worker):
    def __init__(self, queues=None, *args, **kwargs):
        super().__init__(queues, *args, **kwargs)

def start_worker():
    print('worker sleeping for 5 sec before starting...')
    time.sleep(5)

    print('starting worker...')

    _load_index()

    _r = Redis(host='redis', port=6379)
    _q = Queue(connection=_r)

    with Connection(connection=_r):
        w = KmWorker(queues=_q)
        w.work()

def _load_index():
    # connect to the disk index
    the_index = Index(li.pubmed_path)
    li.the_index = the_index