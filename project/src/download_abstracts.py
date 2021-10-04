import ftplib
import os
import os.path as path
from . import km_util as util

# TODO: delete/update old files if date or bytes is different
# TODO: unit tests
def bulk_download(ftp_address: str, ftp_dir: str, local_dir: str):
    """Bulk download files from FTP server. The server can disconnect 
    without warning, which results in an EOF exception and an empty
    (zero byte) file written. In this case, the script will re-connect,
    remove the empty file, and start downloading files again."""

    # create local directory if it doesn't exist yet
    if not os.path.exists(local_dir):
        os.mkdir(local_dir)

    # get list of files to download
    remote_files_to_get = list_files_to_download(ftp_address, ftp_dir, 
        local_dir)
    n_downloaded = 0

    print('Need to download ' + str(len(remote_files_to_get)) + ' files'
        + ' from ' + ftp_address + '/' + ftp_dir)

    # download the files
    while True:
        try:
            # connect to server and navigate to directory to download from
            ftp = connect_to_ftp_server(ftp_address, ftp_dir)

            for remote_filename in remote_files_to_get:
                local_filepath = path.join(local_dir, remote_filename)
                remove_empty_file(local_filepath)
                
                if not path.exists(local_filepath):
                    download_file(local_dir, remote_filename, ftp)
                    n_downloaded += 1
                    util.report_progress(n_downloaded, len(remote_files_to_get))

            if n_downloaded == len(remote_files_to_get):
                if n_downloaded > 0:
                    print('\n')
                break

        # handle server disconnections
        except EOFError:
            pass

    # log out of FTP server
    ftp.quit()

def remove_empty_file(filename: str):
    if path.exists(filename) and path.getsize(filename) < 1:
        os.remove(filename)

def connect_to_ftp_server(ftp_address: str, ftp_dir: str):
    ftp = ftplib.FTP(ftp_address)
    ftp.login()
    ftp.cwd(ftp_dir)
    return ftp

def list_files_to_download(ftp_address: str, ftp_dir: str, local_dir: str):
    files_to_download = []

    ftp = connect_to_ftp_server(ftp_address, ftp_dir)
    remote_filenames = ftp.nlst()

    for remote_filename in remote_filenames:
        local_filename = path.join(local_dir, remote_filename)
        remove_empty_file(local_filename)

        if not path.exists(local_filename):
            files_to_download.append(remote_filename)

    ftp.quit()
    return files_to_download

def download_file(local_dir: str, remote_filename: str, ftp) -> None:
    local_filename = path.join(local_dir, remote_filename)
         
    with open(local_filename, 'wb') as f:
        ftp.retrbinary("RETR " + remote_filename, f.write)