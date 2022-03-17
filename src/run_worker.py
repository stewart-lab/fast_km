import multiprocessing
import time
import argparse
from workers.km_worker import start_worker
import workers.loaded_index as li

parser = argparse.ArgumentParser()
parser.add_argument('-w', '--workers', default=1)
args = parser.parse_args()

def start_workers(do_multiprocessing = True):
    n_workers = args.workers

    if type(n_workers) is str:
        n_workers = int(n_workers)

    if do_multiprocessing:
        worker_processes = []
        for i in range(0, n_workers):
            p = multiprocessing.Process(target=start_worker)
            worker_processes.append(p)
            p.start()

        while True:
            # if a worker process is dead, restart it
            time.sleep(5)
            for i, worker in enumerate(worker_processes):
                if not worker or not worker.is_alive(): 
                    p = multiprocessing.Process(target=start_worker)
                    worker_processes[i] = p
                    p.start()
            
    else:
        start_worker()

def main():
    print('workers waiting 10 sec for redis to set up...')
    time.sleep(10)
    li.pubmed_path = '/mnt/pubmed'

    start_workers()

if __name__ == '__main__':
    main()