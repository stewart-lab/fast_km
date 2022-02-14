import os
from indexing import download_abstracts as dl
from indexing import km_util as util

def test_download_file(tmp_path):
    local_dir = os.path.join(tmp_path, 'Download')
    ftp_address = 'ftp.ncbi.nlm.nih.gov'
    ftp_dir = 'pubmed/pubmedcommons'

    # connect to FTP server
    connection = dl.connect_to_ftp_server(ftp_address, ftp_dir)

    # download a file
    dl.download_file(local_dir, "README.txt", connection)

    # check that a file was downloaded
    local_file = os.path.join(local_dir, 'README.txt')
    assert os.path.exists(local_file)

    # quit connection to FTP server
    connection.quit()

def test_remove_empty_file(tmp_path):
    local_file = os.path.join(tmp_path, 'delete_me.txt')
    assert not os.path.exists(local_file)

    # make the file
    with open(local_file, 'w'):
        pass
    assert os.path.exists(local_file)

    # delete the file if <1 byte (should be deleted)
    dl.remove_empty_file(local_file)
    assert not os.path.exists(local_file)

def test_remove_not_empty_file(tmp_path):
    local_file = os.path.join(tmp_path, 'dont_delete_me.txt')
    assert not os.path.exists(local_file)

    # make the file
    text = ["text"]
    util.write_all_lines(local_file, text)
    assert os.path.exists(local_file)

    # delete the file if <1 byte (should NOT be deleted)
    dl.remove_empty_file(local_file)
    assert os.path.exists(local_file)

def test_list_files_to_download(tmp_path):
    local_dir = os.path.join(tmp_path, 'Download')
    ftp_address = 'ftp.ncbi.nlm.nih.gov'
    ftp_dir = 'pubmed/baseline'

    os.mkdir(local_dir)
    files = dl.list_files_to_download(ftp_address, ftp_dir, local_dir)
    assert len(files) >= 1000

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
    local_file = os.path.join(local_dir, 'commons_archive.csv')
    assert os.path.exists(local_file)

    # there should be no remaining files to download 
    # from ftp.ncbi.nlm.nih.gov/pubmed/pubmedcommons
    files_remaining = dl.list_files_to_download(
        ftp_address, ftp_dir, local_dir)
    assert not files_remaining