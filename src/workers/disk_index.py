import mmap
import pygtrie
import pickle

delim = '\t'

class DiskIndex():
    def __init__(self, bin_path: str, txt_path: str):
        self.bin_path = bin_path
        self.txt_path = txt_path
        self.offset_trie = pygtrie.StringTrie()
        self.token_cache = {}
        self._init_byte_info()
        self._start_connection()

    def close_connection(self):
        self.connection.close()
        self.file_obj.close()

    def query_index(self, token: str):
        if token not in self.offset_trie:
            self.token_cache[token] = {}
        elif token not in self.token_cache:
            byte_info = self.offset_trie[token]
            byte_offset = byte_info[0]
            byte_len = byte_info[1]

            self.connection.seek(byte_offset)
            stored_bytes = self.connection.read(byte_len)

            deserialized = pickle.loads(stored_bytes)
            self.token_cache[token] = deserialized

        return self.token_cache[token]

    def _start_connection(self):
        self.file_obj = open(self.bin_path, mode='rb')
        self.connection = mmap.mmap(self.file_obj.fileno(), length=0, access=mmap.ACCESS_READ)

    def _init_byte_info(self):
        with open(self.txt_path, 'r') as t:
            for index, line in enumerate(t):
                split = line.split(sep=delim)
                key = split[0]
                byte_offset = int(split[1].strip())
                byte_len = int(split[2].strip())

                self.offset_trie[key] = (byte_offset, byte_len)

def write_byte_info(path_bin: str, path_txt: str, trie: pygtrie.StringTrie):
    n_bytes = 0

    with open(path_bin, 'wb') as b:
        with open(path_txt, 'w') as t:
            for key in trie.keys():
                value = trie[key]

                t.write(key)
                t.write(delim)
                t.write(str(n_bytes))
                t.write(delim)
                t.write(str(len(value)))
                n_bytes += len(value)
                t.write('\n')

                b.write(value)