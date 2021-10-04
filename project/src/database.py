import sqlite3
import pickle
from io import BytesIO

class Database():
    def __init__(self, path_to_db: str):
        self.connection = sqlite3.connect(path_to_db)
        self._initialize_ngram_table()
        self._initialize_indexed_files_table()
        self._initialize_publication_year_table()

    def _initialize_ngram_table(self) -> None:
        sql = """ 
            CREATE TABLE IF NOT EXISTS ngrams_to_pmids (
            ngram TEXT PRIMARY KEY,
            pmid_set BLOB NOT NULL
            ); 
            """
        self.connection.execute(sql)

        sql = """ 
            CREATE UNIQUE INDEX IF NOT EXISTS index_name
            ON ngrams_to_pmids (ngram);
            """
        self.connection.execute(sql)

    def _initialize_indexed_files_table(self) -> None:
        sql = """ 
            CREATE TABLE IF NOT EXISTS indexed_files (
            filename TEXT PRIMARY KEY
            ); 
            """
        self.connection.execute(sql)

    def _initialize_publication_year_table(self) -> None:
        sql = """ 
            CREATE TABLE IF NOT EXISTS publication_years (
            pmid INT PRIMARY KEY,
            year INT NOT NULL
            ); 
            """
        self.connection.execute(sql)

    def __del__(self):
        self.connection.close()

    def query(self, query: str) -> 'set[int]':
        cursor = self.connection.execute("SELECT pmid_set FROM ngrams_to_pmids WHERE ngram = ?", (query,))
        rows = cursor.fetchall()

        if len(rows) == 0:
            the_set = set()
        else:
            the_set = pickle.loads(rows[0][0])

        return the_set

    def ngrams(self):
        cursor = self.connection.execute("SELECT ngram FROM ngrams_to_pmids")
        items = cursor.fetchall()
        the_list = []

        for item in items:
            the_list.append(item[0])
        return the_list

    def get_indexed_abstracts_files(self):
        cursor = self.connection.execute("SELECT filename FROM indexed_files")
        items = cursor.fetchall()
        the_list = []

        for item in items:
            the_list.append(item[0])
        return the_list

    def get_pub_years(self) -> dict:
        cursor = self.connection.execute("SELECT pmid, year FROM publication_years")
        items = cursor.fetchall()

        the_dict = {}
        for item in items:
            the_dict[item[0]] = item[1]
        return the_dict

    def dump_cache(self, ngrams_with_pmids: dict, pub_years: dict, indexed_filenames: list) -> None:
        # save the index cache to the db
        self._save_ngrams(ngrams_with_pmids)
        self.save_pub_year(pub_years)
        self.save_indexed_filename(indexed_filenames)

    def _save_ngrams(self, ngrams_to_pmids: dict):
        
        n_grams_to_update = []
        for ngram in ngrams_to_pmids:
            n_grams_to_update.append((ngram,))

        if not n_grams_to_update:
            return

        # update existing records
        tuples = []
        select_query = "SELECT ngram, pmid_set FROM ngrams_to_pmids WHERE ngram = ?"

        for item in n_grams_to_update:
            cursor = self.connection.execute(select_query, item)
            row = cursor.fetchone()

            if not row:
                continue

            ngram = row[0]
            old_set_string = row[1]

            new_set = ngrams_to_pmids[ngram]
            appended = old_set_string + pickle.dumps(new_set)
            tuples.append((ngram, appended))
            del ngrams_to_pmids[ngram]

        # insert n-grams that have no existing record
        for ngram in ngrams_to_pmids:
            the_set = ngrams_to_pmids[ngram]
            tuples.append((ngram, pickle.dumps(the_set)))

        self.connection.executemany(
            'REPLACE INTO ngrams_to_pmids (ngram, pmid_set) VALUES (?, ?)', 
            tuples
        )
        self.connection.commit()

    def save_pub_year(self, pub_years) -> None:
        tuples = []

        for pmid in pub_years:
            tuples.append((pmid, pub_years[pmid]))

        if not tuples:
            return

        self.connection.executemany(
            'REPLACE INTO publication_years (pmid, year) VALUES (?, ?)', 
            tuples
        )
        self.connection.commit()

    def save_indexed_filename(self, filenames) -> None:
        tuples = []

        for file in filenames:
            tuples.append((file,))

        if not tuples:
            return

        self.connection.executemany(
            'REPLACE INTO indexed_files (filename) VALUES (?)',
            tuples
        )
        self.connection.commit()

    def combine_ngram_sets(self):
        tuples = []

        cursor = self.connection.execute("SELECT ngram, pmid_set FROM ngrams_to_pmids")
        
        while True:
            rows = cursor.fetchmany(10000)

            if not rows:
                break

            for item in rows:
                ngram = item[0]
                p = item[1]
                combined_set = set()
                s = BytesIO(p)

                while True:
                    try:
                        partial_set = pickle.load(s)
                        combined_set = combined_set | partial_set
                    except EOFError:
                        pass
                        break

                combined_set_pickled = pickle.dumps(combined_set)
                tuples.append((ngram, combined_set_pickled))

        self.connection.executemany(
            'REPLACE INTO ngrams_to_pmids (ngram, pmid_set) VALUES (?, ?)', 
            tuples
        )
        self.connection.commit()