from redis import Redis
from rq import Worker, Queue, Connection
from indexing.index import Index
import workers.loaded_index as li

_r = Redis(host='redis', port=6379)
_q = Queue(connection=_r)

def start_worker():
    with Connection(connection=_r):
        w = KmWorker(queues=_q)
        w.work()

class KmWorker(Worker):
    def __init__(self, queues=None, *args, **kwargs):
        li.the_index = Index("/mnt/pubmed/Index/db.db")

        super().__init__(queues, *args, **kwargs)