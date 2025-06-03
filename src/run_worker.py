import multiprocessing
import time
import argparse
import os
from workers.km_worker import start_worker
import workers.loaded_index as li
import indexing.km_util as km_util

parser = argparse.ArgumentParser()
parser.add_argument('-w', '--workers', default=1)
parser.add_argument('--high_priority', default=0, required=False)
parser.add_argument('--medium_priority', default=0, required=False)
parser.add_argument('--low_priority', default=0, required=False)
parser.add_argument('--neo4j_address', default='neo4j:7687', required=False)
parser.add_argument('--redis_address', default='redis:6379', required=False)
parser.add_argument('--mongo_address', default='mongo:27017', required=False)
parser.add_argument('--pubmed_api_key', default=os.environ.get('PUBMED_API_KEY', ''), required=False)
parser.add_argument('--openai_api_key',default=os.environ.get('OPENAI_API_KEY', ''), required=False)
parser.add_argument('--htcondor_token', default=os.environ.get('HTCONDOR_TOKEN', ''), required=False)
parser.add_argument('--deepseek_api_key', default=os.environ.get('DEEPSEEK_API_KEY', ''), required=False)
args = parser.parse_args()

def start_workers(do_multiprocessing = True):
    n_workers = int(args.workers)
    high_priority = int(args.high_priority)
    medium_priority = int(args.medium_priority)
    low_priority = int(args.low_priority)
    km_util.redis_address = args.redis_address
    km_util.mongo_address = args.mongo_address
    km_util.neo4j_addresses = [x.strip() for x in args.neo4j_address.split(',')]
    km_util.pubmed_api_key = args.pubmed_api_key
    km_util.openai_api_key = args.openai_api_key
    km_util.htcondor_token = args.htcondor_token
    km_util.deepseek_api_key = args.deepseek_api_key

    if do_multiprocessing:
        worker_processes = []
        for i in range(0, n_workers):
            queue_names = get_worker_queue_names(i, high_priority, medium_priority, low_priority)
            p = multiprocessing.Process(target=start_worker, args=(queue_names,))
            worker_processes.append(p)
            p.start()

        while True:
            # if a worker process is dead, restart it
            time.sleep(5)
            for i, worker in enumerate(worker_processes):
                if not worker or not worker.is_alive(): 
                    queue_names = get_worker_queue_names(i, high_priority, medium_priority, low_priority)
                    p = multiprocessing.Process(target=start_worker, args=(queue_names,))
                    worker_processes[i] = p
                    p.start()
            
    else:
        queue_names = get_worker_queue_names(i, high_priority, medium_priority, low_priority)
        start_worker(queue_names)

def get_worker_queue_names(i, high, med, low):
    if i < high:
        return [km_util.JobPriority.HIGH.name]
    elif i < high + med:
        return [km_util.JobPriority.MEDIUM.name]
    elif i < high + med + low:
        return [km_util.JobPriority.LOW.name]
    else:
        return [x.name for x in km_util.JobPriority]

def main():
    print('INFO: workers waiting 10 sec for redis to set up...')
    time.sleep(10)
    li.pubmed_path = '/mnt/pubmed'

    start_workers()

if __name__ == '__main__':
    main()