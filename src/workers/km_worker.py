from redis import Redis
from rq import Worker, Queue, Connection
import workers.loaded_index as li
from workers.disk_index import DiskIndex

class KmWorker(Worker):
    def __init__(self, queues=None, *args, **kwargs):
        super().__init__(queues, *args, **kwargs)

def start_worker():
    # connect to the disk index
    disk_index = DiskIndex(li.flat_binary_path(), li.flat_text_path())
    li.the_index = disk_index

    _r = Redis(host='redis', port=6379)
    _q = Queue(connection=_r)

    with Connection(connection=_r):
        w = KmWorker(queues=_q)
        w.work()