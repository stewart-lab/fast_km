import os
import nltk
from enum import Enum

redis_host = 'redis'
mongo_host = 'mongo'
neo4j_host = ['neo4j:7687'] # overridden in run_worker.py
tokenizer = nltk.RegexpTokenizer(r"[a-zA-Z0-9Α-Ωα-ω]+")
encoding = 'utf-8'

class JobPriority(Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3

def report_progress(completed: float, total: float) -> None:
    """Shows a progress bar. Adapted from: 
    https://stackoverflow.com/questions/3160699/python-progress-bar"""
    
    progress = completed / total
    bar_length = 20
    block = int(round(bar_length * progress))
    text = "\rProgress: [{0}] {1}% ({2}/{3})".format(
        "█" * block + "-" * (bar_length - block), 
        round(progress * 100),
        int(completed),
        int(total))
    #sys.stdout.write(text)
    #sys.stdout.flush()
    print(text)

    if completed == total:
        print("\n")

def read_all_lines(path: str) -> 'list[str]':
    """Reads a text file into a list of strings"""

    with open(path, 'r', encoding=encoding) as f:
        lines = f.readlines()
        lines = [line.strip("\n\r") for line in lines]

    return lines

def write_all_lines(path: str, items: 'list[str]') -> None:
    """Writes a list of strings to a file"""

    dir = os.path.dirname(path)

    if not os.path.exists(dir):
        os.mkdir(dir)

    with open(path, 'w', encoding=encoding) as f:
        for item in items:
            f.write(str(item))
            f.write('\n')

def get_tokens(text: str) -> 'list[str]':
    l_text = text.lower()
    tokens = tokenizer.tokenize(l_text)
    return tokens

def sanitize_text(text: str) -> str:
    return str.join(' ', get_tokens(text))

def get_index_dir(abstracts_dir: str) -> str:
    return os.path.join(abstracts_dir, 'Index')

def get_abstract_catalog(abstracts_dir: str) -> str:
    return os.path.join(get_index_dir(abstracts_dir), 'abstracts.txt.gzip')

def get_index_file(abstracts_dir: str) -> str:
    return os.path.join(get_index_dir(abstracts_dir), 'index.cdb')

def get_cataloged_files(abstracts_dir: str) -> str:
    return os.path.join(get_index_dir(abstracts_dir), 'cataloged.txt')

def get_knowledge_graph_node_id_index(abstracts_dir: str, graph_name: str) -> str:
    return os.path.join(get_index_dir(abstracts_dir), graph_name + '_node_ids.txt')
    
def get_icite_file(abstracts_dir: str) -> str:
    return os.path.join(get_index_dir(abstracts_dir), 'icite.json')