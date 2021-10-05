import project.src.download_abstracts as downloader
import project.src.index_abstracts as indexer
import project.src.kinderminer as km
import project.src.index as index

# TODO: add parameters
def main():
    """"""
    print('Input local directory path to download files to: ')
    abstracts_dir = input()

    # download baseline
    print('Checking for files to download...')

    downloader.bulk_download(
        ftp_address='ftp.ncbi.nlm.nih.gov',
        ftp_dir='pubmed/baseline',
        local_dir=abstracts_dir)

    # download daily updates
    downloader.bulk_download(
        ftp_address='ftp.ncbi.nlm.nih.gov',
        ftp_dir='pubmed/updatefiles',
        local_dir=abstracts_dir)

    # create/load the index
    the_index = indexer.index_abstracts(abstracts_dir)

    # run kinderminer queries
    while True:
        print('Enter term A. Enter QUIT to quit.')
        a_term = input()

        if a_term == 'QUIT':
            return

        print('Enter term B.')
        b_term = input()

        km_result = km.kinderminer_search(a_term, b_term, the_index)
        print('p-value: ' + str(km_result[0]))
        print('sort ratio: ' + str(km_result[1]))
        print('search time: ' + str(round(km_result[2] * 1000, 0)) + "ms")

# run the main method
if __name__ == '__main__':
    main()