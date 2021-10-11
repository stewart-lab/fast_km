import sqlite3
from . import km_util as util

token_table = 'token_table'
filename_table = 'filename_table'
token_header = 'token'
pmid_header = 'pmid'
loc_header = 'position'
filename_header = 'filename'

# TODO: get list of abstracts in which n-gram occurs. be careful of n-grams w/ a frequently occuring word(s) like "the mouse"
# TODO: figure out how to save items to DB. after every .xml.gz file? lots of strings to put in... (7 billion words)


class Database():
    def __init__(self, path_to_db: str):
        self.connection = sqlite3.connect(path_to_db)
        self._initialize_ngram_table()
        self._initialize_indexed_files_table()

    def __del__(self):
        self.connection.close()
    
    #def index_db(self) -> None:
    #    sql = ("CREATE UNIQUE INDEX IF NOT EXISTS index_name ON "
    #        + token_table + " (" + token_header + ", "
    #        + pmid_header + ");")
    #    self.connection.execute(sql)

    def get_word_loc(self, query: str, pmid: int) -> 'set[int]':
        rows = self.select_items(query, pmid)

        locations = []

        for row in rows:
            locations.append(row[0])

        return locations

    def select_items(self, query: str, pmid: int):
        cursor = self.connection.execute("SELECT " + loc_header + 
            " FROM " + token_table + " WHERE " + token_header + 
            " = ? AND " + pmid_header + " = ?;", (query, pmid,))

        return cursor.fetchall()

    def get_indexed_abstracts_files(self) -> 'list[str]':
        cursor = self.connection.execute("SELECT " + filename_header + 
            " FROM " + filename_table)
        items = cursor.fetchall()
        the_list = []

        for item in items:
            the_list.append(item[0])
        return the_list

    def dump_cache(self, tokens, pmid, indexed_filenames) -> None:
        # save the index cache to the db
        self._save_tokens(tokens, pmid)
        self._save_indexed_filenames(indexed_filenames)

    def query(self, tokens: 'list[str]', pmids: 'set[int]') -> 'set[int]':
        """Checks the input PMIDs to see if the token list appears
        contiguously in them"""
        result = set()

        for token in tokens:
            for pmid in pmids:
                rows = self.select_items(token, pmid)

        return result

    def _initialize_ngram_table(self) -> None:
        sql = ("CREATE TABLE IF NOT EXISTS " + token_table + " (" +
            token_header + " TEXT, " + 
            pmid_header + " INT NOT NULL, " +
            loc_header + " INT NOT NULL);")
        self.connection.execute(sql)
        #PRIMARY KEY, " +

    def _initialize_indexed_files_table(self) -> None:
        sql = ("CREATE TABLE IF NOT EXISTS " + filename_table + " (" +
            filename_header + " TEXT);") # PRIMARY KEY);")
        self.connection.execute(sql)

    def _save_tokens(self, tokens: list, pmid: int) -> None:
        tuples = []
        for i, token in enumerate(tokens):
            tuples.append((token, pmid, i,))

        if not tuples:
            return

        self.connection.executemany(
            "INSERT INTO " + token_table + " (" + token_header + 
            ", " + pmid_header + ", " + loc_header + ") VALUES (?, ?, ?)", 
            tuples
        )
        self.connection.commit()

    def _save_indexed_filenames(self, filenames: list) -> None:
        tuples = []

        for file in filenames:
            tuples.append((file,))

        if not tuples:
            return

        self.connection.executemany(
            "INSERT INTO " + filename_table + " (" + filename_header + 
            ") VALUES (?)",
            tuples
        )
        self.connection.commit()