import multiprocessing
from rq import Worker
from src.redis_conn import redis_conn

def run_workers(low: int, medium: int, high: int, indexing: int):
    queue_names = []
    queue_names += ['LOW'] * low
    queue_names += ['MEDIUM'] * medium
    queue_names += ['HIGH'] * high
    queue_names += ['INDEXING'] * indexing

    for queue in queue_names:
        p = multiprocessing.Process(target=_run_worker, args=(queue,))
        p.start()

def _run_worker(queue_name: str):
    '''Runs a worker that listens to the specified queue.'''
    w = Worker([queue_name], connection=redis_conn)
    w.work()