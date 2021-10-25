import os
import indexing.index_abstracts as indexer

pubmed_path = ''

def index_path():
    return indexer.get_db_path(pubmed_path)

the_index = None





# DEBUG
shared_mem = None