# provides an interface for looking up items in a shared memory dictionary
import multiprocessing
import os
import time
import numpy as np
from multiprocessing.shared_memory import SharedMemory as memory

d_type = np.int32
int_size = np.dtype(d_type).itemsize  # int32 size in bits (32)

class SMA:
    def __init__(self, name: str, create = True):
        self.mem = memory(create=create, size=1024, name=name)

        if create:
            print("created a new shared memory block")
            the_list = [10, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
            self.the_dict = np.array(the_list, dtype=d_type) # dict length + keys + pointers to values
            d_shape = (len(the_list),)
            a = np.ndarray(shape=d_shape, dtype=d_type, buffer=self.mem.buf)
            a[:] = self.the_dict[:]
        else:
            print("accessed an existing shared memory block")

    def iterate(self):
        d_shape = (_len(self.mem),)
        np_array = np.ndarray(shape=d_shape, dtype=d_type, buffer=self.mem.buf, offset=int_size)
        
        for i in range(0, d_shape[0]):
            num = np_array[i]
            print(str(os.getpid()) + "; " + str(num))
            time.sleep(1)

    #def trygetvalue(self, token):
        # get position of query in dictionary backing-array
    #    loc_0 = get_loc_of_(self.the_dict)

    #    length = _len(loc_0)

    #    if length == 0:
    #        return None

    #    hash = _hash(token)
    #    token_loc = _address(hash, length)
    #    the_str = _read_memory(token_loc)

    #    return the_str

    #def add(self):
    #    pass

def get_connection(self, name: str):
    return SMA(name, False)

def _read_memory(shm: memory, loc: int, len: int) -> np.array:
    b = np.ndarray(dtype=d_type, buffer=shm.buf)
    return b

def _len(shm: memory) -> int:
    b = np.ndarray(shape=(1,), dtype=d_type, buffer=shm.buf)
    return b[0] # first element is length of stored array/str

def _offset(hash, length):
    return hash % length

def _address(start, offset):
    return start + offset

def _hash(obj) -> int:
    return hash(obj) # just use the python hash function