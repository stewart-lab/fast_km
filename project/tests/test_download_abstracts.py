import os
from ..src import download_abstracts as dl

def test_bulk_download(tmp_path):
    local_dir = os.path.join(tmp_path, 'Download')
    ftp_address = 'ftp.ncbi.nlm.nih.gov'
    ftp_dir = 'pubmed/pubmedcommons'

    # download the contents of ftp.ncbi.nlm.nih.gov/pubmed/pubmedcommons
    dl.bulk_download(
        ftp_address, 
        ftp_dir, 
        local_dir)

    # check that a file was downloaded
    local_file = os.path.join(local_dir, 'README.txt')
    assert os.path.exists(local_file)

    # there should be no remaining files to download 
    # from ftp.ncbi.nlm.nih.gov/pubmed/pubmedcommons
    files_remaining = dl.list_files_to_download(
        ftp_address, ftp_dir, local_dir)
    assert not files_remaining