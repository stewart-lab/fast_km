from rq import Worker
import workers.loaded_index as li
from workers.shared_memory_access import SMA

from multiprocessing.managers import SharedMemoryManager

shared_memory_name = "sharedmemorytest"

def _create_shared_memory():
    manager = SharedMemoryManager()
    ns = manager.name


    sma = SMA(shared_memory_name)
    return sma

def clean_up_memory(smd: SMA):
    smd.mem.close()
    smd.mem.unlink()

class KmWorker(Worker):
    def __init__(self, queues=None, *args, **kwargs):
        super().__init__(queues, *args, **kwargs)
        li.shared_mem = SMA(shared_memory_name, create=False)