import os
import glob
import gzip
from typing import Iterable
import nltk
import xml.etree.ElementTree as ET
from . import km_util as util
from .index import Index
from .abstract import Abstract

tokenizer = nltk.RegexpTokenizer(r"\w+")

def get_index_dir(abstracts_dir: str) -> str:
    return os.path.join(abstracts_dir, 'Index')

def get_db_path(abstracts_dir: str) -> str:
    return os.path.join(get_index_dir(abstracts_dir), 'db.db')

def get_files_to_index(abstracts_dir: str, already_indexed: Iterable) -> 'list[str]':
    all_abstracts_files = glob.glob(os.path.join(abstracts_dir, "*.xml.gz"))
    not_indexed_yet = []

    for file in all_abstracts_files:
        if file not in already_indexed:
            not_indexed_yet.append(file)

    return not_indexed_yet

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
# TODO: handle synonyms
def index_abstracts(abstracts_dir: str, n_per_cache_dump=10, n=1) -> Index:
    """"""
    print('Building index...')

    the_index = Index(get_db_path(abstracts_dir))
    already_indexed_files = the_index.list_indexed_files()
    files_to_index = get_files_to_index(abstracts_dir, already_indexed_files)

    if not files_to_index:
        print('Done reading existing index')
        return the_index

    i = 0
    util.report_progress(i, len(files_to_index))
    the_index.start_building_index()
    memory_buffer = []

    for gzip_file in files_to_index:
        with gzip.open(gzip_file, 'rb') as file:
            abstracts = parse_xml(file.read())

            for abs in abstracts:
                ngrams = get_n_grams(abs.text, n, memory_buffer)

                for ngram in ngrams:
                    the_index.place_value(ngram, abs.pmid, abs.pub_year)

            the_index._indexed_filenames.add(gzip_file)
            i = i + 1
            util.report_progress(i, len(files_to_index))

        if i % n_per_cache_dump == 0:
            the_index.dump_cache_to_db()

    the_index.finish_building_index()
    print('\n')
    print('Done building index')
    return the_index
