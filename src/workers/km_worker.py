from redis import Redis
from rq import Worker, Queue, Connection
import workers.loaded_index as li
from indexing.index import Index

class KmWorker(Worker):
    def __init__(self, queues=None, *args, **kwargs):
        super().__init__(queues, *args, **kwargs)

def start_worker():
    # connect to the disk index
    the_index = Index(li.pubmed_path)
    li.the_index = the_index

    _r = Redis(host='redis', port=6379)
    _q = Queue(connection=_r)

    with Connection(connection=_r):
        w = KmWorker(queues=_q)
        w.work()