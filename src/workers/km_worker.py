from redis import Redis
from rq import Worker, Queue, Connection
from indexing.index import Index
import workers.loaded_index as li
import time
import indexing.km_util as km_util

class KmWorker(Worker):
    def __init__(self, queues=None, *args, **kwargs):
        super().__init__(queues, *args, **kwargs)

def start_worker(queues: 'list[str]' = [km_util.JobPriority.MEDIUM.name], neo4j_addresses: 'list[str]' = ['neo4j']):
    print('INFO: worker sleeping for 5 sec before starting...')
    time.sleep(5)

    print('INFO: starting worker...')

    _load_index()

    redis_host = km_util.redis_address.split(':')[0]
    redis_port = int(km_util.redis_address.split(':')[1])
    _r = Redis(host=redis_host, port=redis_port)
    _qs = []
    for queue_name in queues:
        _qs.append(Queue(name=queue_name, connection=_r))

    with Connection(connection=_r):
        w = KmWorker(queues=_qs)
        w.work()

def _load_index():
    # connect to the disk index
    the_index = Index(li.pubmed_path)
    li.the_index = the_index