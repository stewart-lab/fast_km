import pygtrie
import numpy as np
import os
from multiprocessing.shared_memory import SharedMemory as memory
import pickle

delim = '\t'
int_size = np.dtype(np.int32).itemsize

class SharedMemoryIndex():
    def __init__(self, txt_path: str, file_bin_path: str, allocate: bool):
        self.txt_path = txt_path
        self.file_bin_path = file_bin_path
        self._trie = pygtrie.StringTrie()

        if allocate:
            self._allocate_shared_mem()
        else:
            self._attach_to_shared_mem()

        print("pid " + str(os.getpid()) + " ready for work")

    def query(self, token):
        print("query " + token + " from pid: " + str(os.getpid()))

        mem = self._trie[token]
        size = np.ndarray(shape=(1,), dtype=np.int32, buffer=mem.buf)[0]
        stored_bytes = np.ndarray(shape=(size,), dtype=np.byte, offset=int_size, buffer=mem.buf)

        deserialized = pickle.loads(stored_bytes)

        return deserialized

    def _allocate_shared_mem(self):
        with open(self.txt_path, 'r') as t:
            with open(self.file_bin_path, 'rb') as b:
                for index, line in enumerate(t):
                    split = line.split(sep=delim)
                    key = split[0]
                    byte_len = int(split[1].strip())
                
                    value = b.read(byte_len)

                    # determine the total byte size of the shared memory array
                    array_nbytes = int_size + len(value)

                    # allocate shared memory
                    try:
                        mem = memory(create=True, size=array_nbytes, name=str(index))
                    except:
                        mem = memory(create=False, name=str(index)) # TODO: resize?

                    # save the length of the serialized dictionary
                    size = np.ndarray(shape=(1,), dtype=np.int32, buffer=mem.buf)
                    size[0] = len(value)

                    # copy the content from the local array into the shared memory array
                    content = np.ndarray(shape=(len(value),), dtype=np.byte, offset=int_size, buffer=mem.buf)
                    the_array = bytearray(value)
                    content[:] = the_array[:]

                    # save a reference to the shared memory location as a value in the trie
                    self._trie[key] = mem

    def _attach_to_shared_mem(self):
        with open(self.txt_path, 'r') as t:
            with open(self.file_bin_path, 'rb') as b:
                for index, line in enumerate(t):
                    split = line.split(sep=delim)
                    key = split[0]

                    # bind to shared memory
                    mem = memory(create=False, name=str(index))

                    # save a reference to the shared memory location as a value in the trie
                    self._trie[key] = mem

def save_trie_to_flat_file(trie: pygtrie.StringTrie, path_bin: str, path_txt: str):
    with open(path_bin, 'wb') as b:
        with open(path_txt, 'w') as t:
            for key in trie:
                val = trie[key]
                val_len = len(val)
                t.write(key)
                t.write(delim)
                t.write(str(len(val)))
                t.write('\n')

                b.write(trie[key])