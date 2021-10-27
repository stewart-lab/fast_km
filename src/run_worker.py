from redis import Redis
from rq import Worker, Queue, Connection
import time
import os
import workers.km_worker as worker
import indexing.download_abstracts as downloader
import indexing.index_abstracts as indexer

def start_worker():
    _r = Redis(host='redis', port=6379)
    _q = Queue(connection=_r)

    with Connection(connection=_r):
        w = worker.KmWorker(queues=_q)
        w.work()

#def main():
#    _r = Redis(host='redis', port=6379)
#    _q = Queue(connection=_r)



from indexing.index import Index
import workers.shared_memory_index as smi
def main():
    test_index = Index('/Users/rmillikin/temp/db.db')
    test_index.place_token('test', 10, 100, 2020)
    test_index.finish_building_index()

    flat_path = '/Users/rmillikin/temp/bin.txt'
    txt_path = '/Users/rmillikin/temp/text.txt'
    #smi.save_trie_to_flat_file(test_index._trie.trie, flat_path, txt_path)

    test_shm_ind = smi.SharedMemoryIndex(txt_path, flat_path, True)
    test_shm_ind_ok = smi.SharedMemoryIndex(txt_path, flat_path, False)
    dict = test_shm_ind_ok.query('test')
    yeah = 0

    for item in test_shm_ind._trie:
        val = test_shm_ind._trie[item]
        val.close()
        val.unlink()

if __name__ == '__main__':
    main()