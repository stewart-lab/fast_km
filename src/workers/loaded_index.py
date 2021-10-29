import os
import indexing.index_abstracts as indexer

pubmed_path = '/mnt/pubmed'

def index_path():
    return indexer.get_db_path(pubmed_path)

def flat_binary_path():
    return os.path.join(pubmed_path, 'Index', 'values.txt')

def flat_text_path():
    return os.path.join(pubmed_path, 'Index', 'keys.txt')

def flat_pub_years_path():
    return os.path.join(pubmed_path, 'Index', 'pub_years.txt')

the_index = None