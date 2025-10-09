import sys
import os
import ftplib
import json
import time
import requests
from src.documents.xml_parsing import read_xml_content, parse_xml_content

port = 8000
km_url = 'http://localhost:' + str(port) + '/api/kinderminer'
hyp_eval_url = 'http://localhost:' + str(port) + '/api/hypothesis_eval'
indexing_url = 'http://localhost:' + str(port) + '/api/index'
doc_url = 'http://localhost:' + str(port) + '/api/documents'

def main():
    # figure out what to download
    max_files = sys.maxsize
    response = requests.get(doc_url + "/origins").json()
    already_downloaded = response['result']
    files_to_download = _get_files_to_download(already_downloaded, max_files)
    print(f"N Files to download: {len(files_to_download)}")

    # add docs
    xml_folder = "_xml"
    os.makedirs(xml_folder, exist_ok=True)
    for file in files_to_download:
        ftp_dir = file[0]
        remote_filename = file[1]
        xml_content = _download_xml(ftp_dir, remote_filename, xml_folder)
        docs = parse_xml_content(xml_content, remote_filename)
        payload = {"documents": [doc.to_dict() for doc in docs]}

        response = requests.post(doc_url, json=payload).json()
        print(f"Added {len(payload['documents'])} documents from {remote_filename}")

    # add icite citation count data if available
    # icite data can be downloaded from: https://nih.figshare.com/collections/iCite_Database_Snapshots_NIH_Open_Citation_Collection_/4586573
    # download the .tar.gz and extract the .json files.
    icite_folder = '_icite'
    if os.path.exists(icite_folder):
        icite_jsons = [f for f in os.listdir(icite_folder) if f.endswith('.json')]
        for icite_json in icite_jsons:
            print(f"Processing {icite_json}...")
            payload = {"documents": []}
            with open(os.path.join(icite_folder, icite_json), 'r') as f:
                for line in f:
                    try:
                        _json = json.loads(line)
                        pmid = _json['pmid']
                        citation_count = _json.get('citation_count', 0)
                        payload_item = {"pmid": pmid, "citation_count": citation_count}
                        payload["documents"].append(payload_item)
                    except json.JSONDecodeError:
                        print(f"Error decoding JSON from line in {icite_json}: {line}")
            
            print(f"Adding citation count data for {len(payload['documents'])} documents from {icite_json}")
            response = requests.post(doc_url, json=payload).json()
            print(f"Response: {response}")
    else:
        print("No _icite folder found, skipping adding citation count data.")
    
    # index docs
    response = requests.post(indexing_url, json={}).json()
    job_id = response['id']
    while True:
        response = requests.get(indexing_url + f"?id={job_id}").json()
        print("Indexing status: ", response)
        if response['status'] in ['finished', 'failed']:
            break
        time.sleep(60)

def _connect_to_ftp_server(ftp_dir: str, ftp_address: str = 'ftp.ncbi.nlm.nih.gov') -> ftplib.FTP:
    """Connect to the FTP server and return the FTP object."""
    ftp = ftplib.FTP(ftp_address)
    ftp.login()
    ftp.cwd(ftp_dir)
    return ftp

def _get_remote_filenames(ftp_dir: str) -> list[str]:
    """Get the list of .xml.gz filenames from the FTP server."""
    ftp = _connect_to_ftp_server(ftp_dir)
    remote_filenames = ftp.nlst()
    ftp.quit()

    # return only .xml.gz files
    return [filename for filename in remote_filenames if filename.endswith('.xml.gz')]

def _get_files_to_download(already_downloaded: list[str], max_files = sys.maxsize) -> list[str]:
    if len(already_downloaded) >= max_files:
        print("Already downloaded max number of files.")
        return []

    baseline = _get_remote_filenames('pubmed/baseline')
    update = _get_remote_filenames('pubmed/updatefiles')
    baseline_to_download = list(set(baseline) - set(already_downloaded))
    update_to_download = list(set(update) - set(already_downloaded))
    baseline_to_download.sort()
    update_to_download.sort()

    max_additional_files = max_files - len(already_downloaded)
    if len(baseline_to_download) + len(update_to_download) > max_additional_files:
        if len(baseline_to_download) >= max_additional_files:
            baseline_to_download = baseline_to_download[:max_additional_files]
            update_to_download = []
        else:
            n_remaining = max_additional_files - len(baseline_to_download)
            update_to_download = update_to_download[:n_remaining]

    return [('pubmed/baseline', f) for f in baseline_to_download] + [('pubmed/updatefiles', f) for f in update_to_download]

def _download_xml(ftp_dir: str, remote_filename: str, xml_dir: str) -> str:
    """Download the .xml.gz file from the FTP server and return its content."""
    local_filename = os.path.join(xml_dir, remote_filename)

    # download the file if not already present
    if not os.path.exists(local_filename):
        ftp = _connect_to_ftp_server(ftp_dir)
        with open(local_filename, 'wb') as f:
            ftp.retrbinary("RETR " + remote_filename, f.write)
        ftp.quit()

    # read content
    xml_content = read_xml_content(local_filename)
    return xml_content

if __name__ == '__main__':
    main()