import ftplib
import os
import math
import glob
from datetime import date
import os.path as path
import indexing.km_util as util

_ftp_lines = []

def connect_to_ftp_server(ftp_address: str, ftp_dir: str):
    """Connects to an FTP server given an FTP address and directory"""

    ftp = ftplib.FTP(ftp_address)
    ftp.login()
    ftp.cwd(ftp_dir)
    return ftp

def download_file(local_dir: str, remote_filename: str, ftp) -> None:
    """Downloads a file via FTP"""

    # create local directory if it doesn't exist yet
    if not os.path.exists(local_dir):
        os.mkdir(local_dir)

    local_filename = path.join(local_dir, remote_filename)
         
    with open(local_filename, 'wb') as f:
        ftp.retrbinary("RETR " + remote_filename, f.write)

def remove_partial_file(filename: str, expected_size: int):
    """Removes partially downloaded file, given an expected file size"""

    if path.exists(filename):
        local_size = path.getsize(filename)
    else:
        local_size = 0

    if local_size != expected_size and path.exists(filename):
        os.remove(filename)
        return filename

    return None

def list_files_to_download(ftp_address: str, ftp_dir: str, local_dir: str):
    """Lists files in the FTP directory that are not in the local directory"""

    files_to_download = []

    ftp = connect_to_ftp_server(ftp_address, ftp_dir)
    remote_filenames = ftp.nlst()

    # determine the size of files on the server and re-download any local files
    # that have only been partially downloaded
    ftp.retrlines('LIST', _retrline_callback)

    # TODO: determine these instead of hardcoding them
    byte_column = 4
    filename_column = 8

    byte_dict = {}
    for line in _ftp_lines:
        split_line = [x for x in str(line).split(' ') if x]
        file_bytes = int(split_line[byte_column])
        file_name = split_line[filename_column]
        byte_dict[file_name] = file_bytes

    for remote_filename in remote_filenames:
        local_filename = path.join(local_dir, remote_filename)
        remote_size = byte_dict[remote_filename]

        if remove_partial_file(local_filename, remote_size):
            print('INFO: partial file found, we will re-download it: ' + local_filename)

        if not path.exists(local_filename):
            files_to_download.append(remote_filename)

    ftp.quit()
    _ftp_lines.clear()
    return files_to_download

def bulk_download(ftp_address: str, ftp_dir: str, local_dir: str, n_to_download = math.inf):
    """Download all files from an FTP server directory. The server can 
    disconnect without warning, which results in an EOF exception and an 
    empty (zero byte) file written. In this case, the script will re-connect,
    remove the empty file, and start downloading files again."""

    if n_to_download == 0:
        return

    # create local directory if it doesn't exist yet
    if not os.path.exists(local_dir):
        os.mkdir(local_dir)

    remote_files_to_get = ['temp']
    n_downloaded = 0

    while remote_files_to_get and n_downloaded < n_to_download:
        # get list of files to download
        remote_files_to_get = list_files_to_download(ftp_address, ftp_dir, 
            local_dir)

        print('INFO: Need to download ' + str(len(remote_files_to_get)) + ' files'
            + ' from ' + ftp_address + '/' + ftp_dir)

        # delete any *.xml.gz* file from previous years
        current_year = int(date.today().strftime("%y"))
        for year in range(10, current_year):
            files_to_remove = glob.glob(os.path.join(local_dir, "pubmed" + str(year) + "*.xml.gz*"))

            for file in files_to_remove:
                if os.path.basename(file) not in remote_files_to_get:
                    print('INFO: deleting outdated file from \'' + str(year) + ': ' + file)
                    os.remove(file)

        # download the files
        try:
            # connect to server and navigate to directory to download from
            ftp = connect_to_ftp_server(ftp_address, ftp_dir)

            for remote_filename in remote_files_to_get:
                if n_downloaded >= n_to_download:
                    break

                local_filepath = path.join(local_dir, remote_filename)

                if not path.exists(local_filepath):
                    download_file(local_dir, remote_filename, ftp)
                    n_downloaded += 1
                    util.report_progress(n_downloaded, len(remote_files_to_get))

        # handle server disconnections
        except EOFError:
            pass

    # log out of FTP server
    ftp.quit()

def _retrline_callback(ftp_line: str):
    global _ftp_lines
    _ftp_lines.append(ftp_line)