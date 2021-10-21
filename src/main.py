import argparse
import server.app as app
import workers.km_worker as worker
import indexing.download_abstracts as downloader
import indexing.index_abstracts as indexer

parser = argparse.ArgumentParser()
parser.add_argument('-w', '--worker', action='store_true')
parser.add_argument('-s', '--server', action='store_true')

def populate_index():
    index_loc = '/mnt/pubmed'
    #downloader.
    #indexer.index_abstracts(index_loc)

def main():
    args = parser.parse_args()
    
    if args.worker and args.server:
        print("must be either worker OR server")
        return

    if args.worker:
        populate_index()
        worker.start_worker()
    if args.server:
        app.start_server()

if __name__ == '__main__':
    main()