import workers.km_worker as worker
import indexing.download_abstracts as downloader
import indexing.index_abstracts as indexer

def populate_index():
    index_loc = '/mnt/pubmed'
    #downloader.
    #indexer.index_abstracts(index_loc)

def main():
    populate_index()
    worker.start_worker()

if __name__ == '__main__':
    main()