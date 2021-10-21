import os
import glob
import gzip
from typing import Iterable
import xml.etree.ElementTree as ET
import indexing.km_util as util
from indexing.index import Index
from indexing.abstract import Abstract

def get_index_dir(abstracts_dir: str) -> str:
    return os.path.join(abstracts_dir, 'Index')

def get_db_path(abstracts_dir: str) -> str:
    return os.path.join(get_index_dir(abstracts_dir), 'db.db')

def get_files_to_index(abstracts_dir: str, already_indexed: Iterable) -> 'list[str]':
    all_abstracts_files = glob.glob(os.path.join(abstracts_dir, "*.xml.gz"))
    not_indexed_yet = []

    for file in all_abstracts_files:
        if os.path.basename(file) not in already_indexed:
            not_indexed_yet.append(file)

    return not_indexed_yet

def parse_xml(xml_content: str) -> 'list[Abstract]':
    """"""
    root = ET.fromstring(xml_content)
    abstracts = []

    for pubmed_article in root.findall('PubmedArticle'):
        try:
            pmid = None
            year = None
            title = None
            full_text = None

            medline_citation = pubmed_article.find('MedlineCitation')
            article = medline_citation.find('Article')
            journal = article.find('Journal')
            journal_issue = journal.find('JournalIssue')
            pub_date = journal_issue.find('PubDate')
            abstract = article.find('Abstract')

            # get PMID
            pmid = medline_citation.find('PMID').text

            # get publication year
            year = pub_date.find('Year').text

            # get article title
            title = article.find('ArticleTitle').text

            # get article abstract text
            abs_text_nodes = abstract.findall('AbstractText')
            if len(abs_text_nodes) == 1:
                full_text = "".join(abs_text_nodes[0].itertext())
            else:
                node_texts = []

                for node in abs_text_nodes:
                    node_texts.append("".join(node.itertext()))
                
                full_text = " ".join(node_texts)

            if (type(full_text) is str) and (type(pmid) is str):
                abstract = Abstract(int(pmid), int(year), title, full_text)
                abstracts.append(abstract)

        except AttributeError:
            pass

    return abstracts

# TODO: unzip .gz.md5
# TODO: verify .xml with .md5
def index_abstracts(abstracts_dir: str, n_per_cache_dump = 10) -> Index:
    """"""
    print('Building index...')

    the_index = Index(get_db_path(abstracts_dir))
    already_indexed_files = the_index.list_indexed_files()
    files_to_index = get_files_to_index(abstracts_dir, already_indexed_files)

    util.report_progress(0, len(files_to_index))

    for i, gzip_file in enumerate(files_to_index):
        with gzip.open(gzip_file, 'rb') as xml_file:
            abstracts = parse_xml(xml_file.read())

            for abstract in abstracts:
                the_index.index_abstract(abstract)

            filename = os.path.basename(gzip_file)
            the_index._indexed_filenames.add(filename)
            i = i + 1
            util.report_progress(i, len(files_to_index))

        if i % n_per_cache_dump == 0:
            the_index.dump_index_to_trie()

    the_index.finish_building_index()
    print('Done building index')
    return the_index
