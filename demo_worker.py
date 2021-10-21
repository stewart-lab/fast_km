from redis import Redis
from rq import Worker, Queue
from project.src.index import Index
import project.src.loaded_index as lindx

r = Redis(host='redis', port=6379)
q = Queue(connection=r)

class DemoWorker(Worker):
    def __init__(self, queues=None, *args, **kwargs):
        lindx.the_index = Index("/mnt/pubmed/Index/db.db")

        super().__init__(queues, *args, **kwargs)