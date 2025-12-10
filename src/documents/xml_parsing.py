import os
import re
import gzip
import xml.etree.ElementTree as ET
from src.documents.document import Document

delim = '\t'
year_regex = r"(?<!\d)(?:1\d\d\d|20\d\d)(?!\d)"

def read_xml_content(local_filename: str) -> str:
    if os.path.exists(local_filename):
        with open(local_filename, 'rb') as f:
            with gzip.GzipFile(fileobj=f) as gz:
                decompressed_data = gz.read()
            xml_content = decompressed_data.decode('utf-8')
        return xml_content
    else:
        raise FileNotFoundError(f"File {local_filename} not found.")

def parse_xml_content(xml_content: str, xml_filename: str) -> list[Document]:
    """Parse the XML content and return a list of Abstract objects."""
    root = ET.fromstring(xml_content)
    abstracts = []

    for pubmed_article in root.findall('PubmedArticle'):
        pmid = None
        year = None
        title = None
        abstract_text = None
        journal = None

        # get PMID
        try:
            medline_citation = pubmed_article.find('MedlineCitation')
            pmid = medline_citation.find('PMID').text
        except AttributeError:
            print("WARNING: Error parsing PMID. Skipping article.")
            continue

        # get publication year
        try:
            article = medline_citation.find('Article')
            journal = article.find('Journal')
            journal_issue = journal.find('JournalIssue')
            pub_date = journal_issue.find('PubDate')
            year = pub_date.find('Year').text
        except AttributeError:
            year = 99999

        if year == 99999:
            try:
                date_completed = medline_citation.find('DateCompleted')
                year = date_completed.find('Year').text
            except AttributeError:
                year = 99999
        
        if year == 99999:
            try:
                article = medline_citation.find('Article')
                journal = article.find('Journal')
                journal_issue = journal.find('JournalIssue')
                pub_date = journal_issue.find('PubDate')
                date_string = pub_date.find('MedlineDate').text
                match = re.search(year_regex, date_string)
                year = match.group()
            except AttributeError:
                year = 99999

        # get article title
        try:
            title = article.find('ArticleTitle')
            title = "".join(title.itertext())
        except AttributeError:
            pass

        # get article abstract text
        try:
            abstract = article.find('Abstract')
            abs_text_nodes = abstract.findall('AbstractText')
            if len(abs_text_nodes) == 1:
                abstract_text = "".join(abs_text_nodes[0].itertext())
            else:
                node_texts = []

                for node in abs_text_nodes:
                    node_texts.append("".join(node.itertext()))
                
                abstract_text = " ".join(node_texts)
        except AttributeError:
            pass

        if pmid:
            abstract = Document(int(pmid), int(year), title, abstract_text, xml_filename)
            abstracts.append(abstract)
        else:
            print("WARNING: PMID not found. Skipping article.")

    return abstracts