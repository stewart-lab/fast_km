import os
import gzip
import pickle
import xml.etree.ElementTree as ET
import indexing.km_util as util
from indexing.token_trie import TokenTrie
from indexing.abstract import Abstract

class Indexer(): 
    def __init__(self, path_to_trie: str):
        self._tokens_with_pmids = dict()
        self._publication_years = dict()
        self.path_to_trie = path_to_trie

        if os.path.exists(path_to_trie):
            self._trie = self._load_existing_trie()
        else:
            self._trie = TokenTrie()

    def index_xml_file(self, gzip_file: str):
        with gzip.open(gzip_file, 'rb') as xml_file:
            abstracts = self._parse_xml(xml_file.read())

            for abstract in abstracts:
                self.index_abstract(abstract)

            filename = os.path.basename(gzip_file)
            self._trie.indexed_abstract_files.add(filename)

    def index_abstract(self, abstract: Abstract):
        tokens = util.get_tokens(abstract.text)

        for i, token in enumerate(tokens):
            self.place_token(token, i, abstract.pmid, abstract.pub_year)
        
        self._publication_years[abstract.pmid] = abstract.pub_year

    def place_token(self, token: str, pos: int, id: int, pub_year: int) -> None:
        l_token = token.lower()

        if l_token not in self._tokens_with_pmids:
            self._tokens_with_pmids[l_token] = dict()

        tokens = self._tokens_with_pmids[l_token]

        if id not in tokens:
            tokens[id] = pos
        elif type(tokens[id]) is int:
            tokens[id] = [tokens[id], pos]
        else: # type is list
            tokens[id].append(pos)
    
    def dump_index_to_trie(self):
        """Call this method while building the index periodically. Saves 
        info from in-memory dictionaries into the database and clears 
        in-memory dictionaries"""
        self._trie.serialize_index(self._tokens_with_pmids, self._publication_years)
        self._tokens_with_pmids = dict()
        self._save_trie_to_disk()

    def finish_building_index(self):
        self.dump_index_to_trie()
        self._trie.combine_serialized_sets()
        self.dump_index_to_trie()
        
    def list_indexed_files(self) -> 'list[str]':
        return self._trie.indexed_abstract_files

    def _load_existing_trie(self):
        with open(self.path_to_trie, 'rb') as handle:
            b = pickle.load(handle)
            return b
        
    def _save_trie_to_disk(self):
        dir = os.path.dirname(self.path_to_trie)
        
        if not os.path.exists(dir):
            os.mkdir(dir)

        temp_path = os.path.join(dir, 'db.temp')
        with open(temp_path, 'wb') as handle:
            pickle.dump(self._trie, handle)

        # delete old index and rename new index
        os.remove(self.path_to_trie)
        os.rename(temp_path, self.path_to_trie)

    def _parse_xml(self, xml_content: str) -> 'list[Abstract]':
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
