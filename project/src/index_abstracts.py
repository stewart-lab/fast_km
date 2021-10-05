import os
import glob
import gzip
import nltk
import xml.etree.ElementTree as ET
from .index import Index
from . import km_util as util

tokenizer = nltk.RegexpTokenizer(r"\w+")
n = 1
n_files_per_cache_dump = 10

# TODO: unzip .gz.md5
# TODO: verify .xml with .md5
def index_abstracts(abstracts_dir: str) -> Index:
    """"""
    print('Building index...')
    the_index = Index(get_db_path(abstracts_dir))

    gzip_files = get_files_to_index(abstracts_dir, the_index)

    if not gzip_files:
        print('Done reading existing index')
        return the_index

    # DEBUG
    #already_indexed = the_index.get_indexed_abstracts_files()
    #if len(already_indexed) >= 10:
    #    print('\n')
    #    print('Done loading debug index')
    #    return the_index
    # END DEBUG

    i = 0
    util.report_progress(i, len(gzip_files))
    the_index.start_building_index()

    for gzip_file in gzip_files:
        with gzip.open(gzip_file, 'rb') as file:
            xml_content = file.read()
            parse_xml(xml_content, the_index)

            the_index._indexed_filenames.append(gzip_file)
            i = i + 1
            util.report_progress(i, len(gzip_files))

        if i % n_files_per_cache_dump == 0:
            the_index.dump_cache_to_db()

    the_index.finish_building_index()
    print('\n')
    print('Done building index')
    return the_index

# TODO: handle synonyms
def parse_xml(xml_content: str, the_index: Index) -> None:
    """"""
    root = ET.fromstring(xml_content)
    memory_buffer = []

    for pubmed_article in root.findall('PubmedArticle'):
        try:
            medline_citation = pubmed_article.find('MedlineCitation')
            article = medline_citation.find('Article')
            abstract = article.find('Abstract')

            # get PMID
            pmid = medline_citation.find('PMID').text

            # get publication year
            date_info = medline_citation.find('DateCompleted')
            year = date_info.find('Year').text

            # get article title
            title = article.find('ArticleTitle').text

            # get article abstract text
            abstract_text = abstract.find('AbstractText').text

            if (type(abstract_text) is str) and (type(pmid) is str):
                for n_gram in get_n_grams(abstract_text, n, memory_buffer):
                    the_index.place_value(n_gram, int(pmid), int(year))

        except AttributeError:
            pass

def get_n_grams(text: str, n: int, n_gram_mem_buffer: list) -> 'list[str]':
    tokens = tokenizer.tokenize(text)
    n_gram_mem_buffer.clear()

    for i, token in enumerate(tokens):
        if n > 1:
            for j in range(i + 1, i + n + 1):
                n_gram_mem_buffer.append(' '.join(tokens[i:j]))
        else:
            n_gram_mem_buffer.append(tokens[i])

    return n_gram_mem_buffer

def get_index_dir(abstracts_dir: str) -> str:
    return os.path.join(abstracts_dir, 'Index')

def get_db_path(abstracts_dir: str) -> str:
    return os.path.join(get_index_dir(abstracts_dir), 'db.db')

def get_files_to_index(abstracts_dir: str, index: Index) -> 'list[str]':
    all_abstracts_files = glob.glob(os.path.join(abstracts_dir, "*.xml.gz"))
    not_indexed_yet = []
    already_indexed = index.get_indexed_abstracts_files()

    for file in all_abstracts_files:
        if file not in already_indexed:
            not_indexed_yet.append(file)

    return not_indexed_yet