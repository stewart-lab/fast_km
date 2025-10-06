import os
import time
import datetime
from typing import Generator
import uuid
import zlib
import sqlite3
import msgpack
import psycopg
import numpy as np
from src.fast_km_exception import FastKmException
from src.documents.document import Document
import src.indexing.indexing_util as util
import src.global_vars as gvars

MIN_CENSOR_YEAR = gvars.MIN_CENSOR_YEAR
MAX_CENSOR_YEAR = gvars.MAX_CENSOR_YEAR
BINARY_TYPE = "BLOB" if not gvars.POSTGRES_HOST else "BYTEA"
PH = "?" if not gvars.POSTGRES_HOST else "%s"

"""
PubMed Abstract Indexing System
================================================
This overview was written by Claude Opus 4.1 and revised by a human for accuracy.

This class implements an inverted index for searching ~40 million PubMed 
abstracts with the goal of achieving:
  - Query latency: <10ms per search (typically 1ms or less)
  - Memory usage: <8GB peak
  - Initial index build: ~24 hours
  - Daily updates: ~1 hour
  - Readable by people with relatively basic Python and SQL knowledge
  - Minimal dependencies (SQLite and msgpack; the latter is a safe/fast alternative to 'pickle')

OVERVIEW
================================================
Initial Build:
  - Download PubMed .xml files (~1600 files) one at a time.
  - A typical .xml file contains 30000 abstracts.
  - Read the abstracts and 'index' their n-grams (uni-grams and bi-grams).
    'Indexing' refers to saving which documents (PMIDs) contain each n-gram.
    This makes it easy to later look up which PMIDs an n-gram occurs in.
    An n-gram is effectively n words in a row. e.g., "pancreatic ductal carcinoma" 
    is a tri-gram (three words).
  - Each n-gram is mapped to a SQL table ('fragmented shard') using a hash function.
  - The PMID/position data is compressed into a binary format with msgpack.
  - This n-gram : PMID/position data is inserted into the fragmented shard table.
    The fragmented shard tables are optimized for fast inserts because there will
    be many millions of inserts total to index the entire PubMed corpus
    (roughly 220 million row inserts, since each abstract generates multiple n-grams).
  - This process is repeated for each .xml file.
  - After all .xml files are processed, there will be many duplicate n-grams in
    each table, and searches would be very slow because the fragmented shard tables
    do not have a SQL index. To enable fast searches, we 'defragment' these tables.
  - 'Defragmentation' is essentially when the multiple rows of n-gram : PMID/position 
    mappings are condensed into (usually) one row. For very common terms like 'the',
    the PMID/position data is broken up into multiple rows because their binary
    data is very large and is costly to load into memory.
  - The defragmented shards (or simply, 'shards') are separate tables that have
    a SQL index on the n-gram for fast lookups.
  - Defragmentation of all fragmented shards is performed after all .xmls are
    processed.

Updates:
  - Updating the index looks very similar to the original building of it.
  - New PubMed update files are downloaded (usually one .xml per day since last update).
  - The abstracts are read and indexed in the same way as the initial build.
    The data is inserted into the fragmented shards.
  - Defragmentation merges the new data with existing shard data.

Query Process:
  - Hash search term to determine its shard (1-950)
  - Query that shard table
  - Decompress the retrieved binary PMID/position data
  - Return the set of PMIDs
  - It's more complex for trigrams and up, see "search_documents" function below

OPTIMIZATION
================================================
  - PubMed .xml files are downloaded and indexed one at a time (RAM optimization).
  - Fragmented shards do not have SQL indexes on them for fast inserts (CPU/time optimization).
  - Very common n-grams like 'the' are stored in multiple rows in the defragmented
    shard (RAM optimization).
  - msgpack instead of pickle (mostly safety, but also RAM/time optimization).
  - When defragmenting, max 100k rows are retrieved from the fragmented shard
    at one time (RAM optimization).
  - Unigrams do not have position information stored with them (RAM/time optimization).
  - Probably goes without saying, but the defragmented shard tables
    have SQL indexes on the n-gram for fast lookups (time optimization).

IMPLEMENTATION NOTES
================================================
- The hash function for shard assignment is deterministic across different 
  machines and operating systems. This allows portability of a .db file.
- The shard count (950) was chosen because SQLite has a max of 2000 tables.
  950 tables are used for the fragmented shards, 950 for defragmented shards,
  and 100 left in case we want to add more tables for other things.
- The defragmentation process is probably not parallelizable because
  it's ~limited by disk I/O.
- There is a metadata table that tracks which .xml file have been indexed to 
  avoid reprocessing.
"""


class Index():
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.n_documents = 0
        self.doc_origins = set[str]()
        self.pmid_to_pub_year = np.zeros(0, dtype=np.uint16) # saves memory vs python dict
        self.pmid_to_citation_count = np.zeros(0, dtype=np.uint32) # saves memory vs python dict

        self._initialize()
        self._corpus = self._get_db_connection("_corpus")
        self._index = self._get_db_connection("_index")

        self._term_cache = dict[str, set[int]]()
        self._ngram_cache = dict[str, dict[int, list[int]]]()
        self._bi_grams_maybe_should_cache = dict[str, int]()
        self._idx_cursor = self._index.cursor()
        self._corpus_cursor = self._corpus.cursor()
        self._load_metadata()

    def add_or_update_documents(self, documents: list[Document]) -> None:
        """Add documents to the corpus"""
        if self.is_indexing_in_progress():
            raise FastKmException("Indexing is currently in progress, or was interrupted. Indexing must be completed before adding new documents.")

        cursor = self._corpus.cursor()
        doc_tables = set(_pmid_to_table_name(doc.pmid) for doc in documents)
        for doc_table in doc_tables:
            # create document table if it doesn't exist
            cursor.execute(f'''
                            CREATE TABLE IF NOT EXISTS {doc_table} (
                                uuid TEXT NOT NULL, 
                                is_indexed INTEGER NOT NULL,
                                pmid INTEGER PRIMARY KEY, 
                                pub_year INTEGER NOT NULL, 
                                title TEXT NOT NULL,
                                abstract TEXT NOT NULL,
                                body TEXT NOT NULL,
                                origin TEXT NOT NULL, 
                                citation_count INTEGER NOT NULL 
                            )''')
            pmids = [doc.pmid for doc in documents if _pmid_to_table_name(doc.pmid) == doc_table]
            batch_size = 1000

            for i in range(0, len(pmids), batch_size):
                batch = pmids[i:i + batch_size]
                batch_pmids = set(batch)
                batch_docs = {doc for doc in documents if doc.pmid in batch_pmids}
                doc_table_inserts = list[tuple]()
                doc_version_table_inserts = list[tuple]()

                # look up existing documents
                cursor.execute(f'''SELECT * FROM {doc_table} WHERE pmid in ({','.join(f'{PH}' for _ in batch)})''', batch)
                current_doc_versions = cursor.fetchall()
                current_versions = {doc[2]: doc for doc in current_doc_versions} # pmid -> row

                for new_doc in batch_docs:
                    # special case. if we're just updating a non-indexed field, just update that field. no need to create a new doc.
                    if new_doc.title is None and new_doc.abstract is None and new_doc.body is None:
                        if new_doc.citation_count is not None:
                            cursor.execute(f'''UPDATE {doc_table} SET citation_count = ? WHERE pmid = ?''', (new_doc.citation_count, new_doc.pmid))
                        if new_doc.pub_year is not None:
                            cursor.execute(f'''UPDATE {doc_table} SET pub_year = ? WHERE pmid = ?''', (new_doc.pub_year, new_doc.pmid))
                        if new_doc.origin is not None:
                            cursor.execute(f'''UPDATE {doc_table} SET origin = ? WHERE pmid = ?''', (new_doc.origin, new_doc.pmid))
                        continue

                    current_version = current_versions.get(new_doc.pmid, None)

                    version_uuid = str(uuid.uuid4())
                    is_indexed = 0

                    if current_version is not None:
                        # update existing doc
                        new_version = (
                            version_uuid,
                            is_indexed,
                            new_doc.pmid,
                            new_doc.pub_year if new_doc.pub_year is not None else current_version[3],
                            new_doc.title if new_doc.title is not None else current_version[4],
                            new_doc.abstract if new_doc.abstract is not None else current_version[5],
                            new_doc.body if new_doc.body is not None else current_version[6],
                            new_doc.origin if new_doc.origin is not None else current_version[7],
                            new_doc.citation_count if new_doc.citation_count is not None else current_version[8]
                        )
                        doc_version_table_inserts.append(new_version)
                        doc_version_table_inserts.append(current_version)
                    else:
                        # no existing version, insert new doc
                        new_version = (
                            version_uuid,
                            is_indexed,
                            new_doc.pmid,
                            new_doc.pub_year,
                            new_doc.title if new_doc.title is not None else "",
                            new_doc.abstract if new_doc.abstract is not None else "",
                            new_doc.body if new_doc.body is not None else "",
                            new_doc.origin if new_doc.origin is not None else "",
                            new_doc.citation_count if new_doc.citation_count is not None else 0
                        )

                    doc_table_inserts.append(new_version)

                # bulk inserts
                if doc_table_inserts:
                    cursor.executemany(f'''
                                INSERT INTO {doc_table} (
                                    uuid, 
                                    is_indexed, 
                                    pmid, 
                                    pub_year, 
                                    title, 
                                    abstract,
                                    body,
                                    origin,
                                    citation_count
                                ) VALUES ({PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH})
                                ON CONFLICT (pmid) DO UPDATE SET
                                    uuid = EXCLUDED.uuid,
                                    is_indexed = EXCLUDED.is_indexed,
                                    pub_year = EXCLUDED.pub_year,
                                    title = EXCLUDED.title,
                                    abstract = EXCLUDED.abstract,
                                    body = EXCLUDED.body,
                                    origin = EXCLUDED.origin,
                                    citation_count = EXCLUDED.citation_count
                            ''', doc_table_inserts)
                if doc_version_table_inserts:
                    cursor.executemany(f'''
                                INSERT INTO document_versions (
                                    uuid, 
                                    is_indexed, 
                                    pmid, 
                                    pub_year, 
                                    title, 
                                    abstract,
                                    body,
                                    origin,
                                    citation_count
                                ) VALUES ({PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH})
                                ON CONFLICT (uuid) DO UPDATE SET
                                    is_indexed = EXCLUDED.is_indexed,
                                    pmid = EXCLUDED.pmid,
                                    pub_year = EXCLUDED.pub_year,
                                    title = EXCLUDED.title,
                                    abstract = EXCLUDED.abstract,
                                    body = EXCLUDED.body,
                                    origin = EXCLUDED.origin,
                                    citation_count = EXCLUDED.citation_count
                            ''', doc_version_table_inserts)

        # update the doc_origins table with new origin name(s) - likely just one .xml file
        origins = {abstract.origin for abstract in documents if abstract.origin}
        if origins:
            cursor.executemany(f'''INSERT INTO doc_origins (origin) VALUES ({PH})''', [(origin,) for origin in origins])

        # commit changes
        self._corpus.commit()

        # update metadata (commits)
        self._refresh_metadata(documents)
        cursor.close()

    def index_documents(self) -> Generator[float, None, None]:
        ## --- determine what needs to be indexed ---
        doc_tables = self._get_tables("documents_%")
        doc_tables.sort()

        n_docs_need_indexing = 0
        doc_tables_need_indexing = []
        for doc_table in doc_tables:
            self._corpus_cursor.execute(f'''SELECT COUNT(pmid) FROM {doc_table} WHERE is_indexed = 0''')
            n_docs_need_indexing_table = self._corpus_cursor.fetchone()[0]
            n_docs_need_indexing += n_docs_need_indexing_table

            if n_docs_need_indexing_table > 0:
                doc_tables_need_indexing.append(doc_table)

        if n_docs_need_indexing == 0:
            return
        
        # count number of shards (fixed at 950 but retrieved here just in case we change it later) - used to estimate time to defrag
        shards = self._get_tables("shard_%")
        n_shards = len(shards)

        # count terms in the disk cache, this is used to estimate how long the cache refresh will take
        self._idx_cursor.execute('''SELECT COUNT(term) FROM query_cache''')
        rows = self._idx_cursor.fetchone()
        cached_term_count = rows[0] if rows else 0

        # estimate time to index, defrag, and refresh cache
        est_time_indexing_per_doc = 0.0003          # rough estimate, 0.3ms per doc
        est_time_defrag_per_shard = 10              # rough estimate, 10 seconds per shard
        est_time_cache_refresh_per_term = 0.005     # rough estimate, 5ms per term
        est_total_time = (est_time_indexing_per_doc * n_docs_need_indexing +
                          est_time_defrag_per_shard * n_shards +
                          est_time_cache_refresh_per_term * cached_term_count)

        ## --- index the documents (builds the fragmented shards) ---
        est_progress_time = 0.0 # in estimated seconds
        progress = 0.0
        # n_indexed = 0
        doc_batch_size = 30_000
        for doc_table in doc_tables_need_indexing:
            # index the documents in this table
            self._corpus_cursor.execute(f'''SELECT pmid, pub_year, title, abstract, origin FROM {doc_table} WHERE is_indexed = 0''')
            while True:
                batch = self._corpus_cursor.fetchmany(doc_batch_size)
                if not batch:
                    break

                index_update = dict()
                for doc_row in batch:
                    doc = Document(pmid=doc_row[0], pub_year=doc_row[1], title=doc_row[2], abstract=doc_row[3], origin=doc_row[4])
                    util.index_document(doc, index_update)

                shard_to_ngrams = dict[str, list[str]]()
                for ngram in index_update:
                    fragmented_shard = _ngram_to_shard_name(ngram, get_fragmented_name=True)
                    if fragmented_shard not in shard_to_ngrams:
                        shard_to_ngrams[fragmented_shard] = []
                    shard_to_ngrams[fragmented_shard].append(ngram)

                for fragmented_shard in sorted(shard_to_ngrams.keys()):
                    ngrams = shard_to_ngrams[fragmented_shard]
                    self._idx_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {fragmented_shard} (ngram TEXT, data {BINARY_TYPE})''')

                    for ngram in ngrams:
                        data_fragment = index_update[ngram]
                        if isinstance(data_fragment, set):
                            data_fragment = list(data_fragment)
                        data_fragment_encoded = msgpack.dumps(data_fragment)
                        # TODO: bulk insert
                        self._idx_cursor.execute(f'''INSERT INTO {fragmented_shard} (ngram, data) VALUES ({PH}, {PH})''', (ngram, data_fragment_encoded))

                # mark the documents as indexed
                update_cursor = self._corpus.cursor()
                for doc_row in batch:
                    pmid = doc_row[0]
                    update_cursor.execute(f'''UPDATE {doc_table} SET is_indexed = 1 WHERE pmid = {PH}''', (pmid,))

                # commit the batch
                self._index.commit()
                self._corpus.commit()
                update_cursor.close()

                est_progress_time += est_time_indexing_per_doc * len(batch)
                progress = est_progress_time / est_total_time
                yield progress


            # handle version conflicts for this table

            # if an abstract has been updated, we need to delete the old version's data from the index.
            # in these cases, there will be a mismatch between the 'is_indexed' columns in the 'documents' and 'document_versions' tables.
            # the 'is_indexed' column in document_versions will be 1 for the old version but the UUID will not match the document in the 'documents' table.
            # this is not the clearest situation, I'd like to make it more explicit at some point.
            pmids_to_reindex = []

            # get the indexed versions
            self._corpus_cursor.execute('''SELECT pmid, uuid FROM document_versions WHERE is_indexed = 1''')
            indexed_versions = {row[0]: row[1] for row in self._corpus_cursor.fetchall()}
            if indexed_versions:
                # get the current versions
                pmids = list(indexed_versions.keys())
                self._corpus_cursor.execute(f'''SELECT pmid, uuid FROM {doc_table} WHERE pmid IN ({','.join(f'{PH}' for _ in pmids)})''', pmids)
                current_versions = {row[0]: row[1] for row in self._corpus_cursor.fetchall()}

                # if indexed UUID != current UUID, then it requires re-indexing
                pmids_to_reindex = [pmid for pmid in pmids if indexed_versions[pmid] != current_versions[pmid]]
            
            if pmids_to_reindex:
                # the current version's data is in the fragmented shard.
                # so all we have to do is delete the (old) indexed version's data from the shard
                # and the defragmentation will take care of the rest.
                self._corpus_cursor.execute('''SELECT pmid, pub_year, title, abstract, origin FROM document_versions WHERE is_indexed = 1 AND pmid IN ({})'''.format(','.join(f'{PH}' for _ in pmids_to_reindex)), pmids_to_reindex)
                while True:
                    batch = self._corpus_cursor.fetchmany(doc_batch_size)
                    if not batch:
                        break

                    # figure out what n-grams we need to update
                    index_update = dict()
                    for doc_row in batch:
                        doc = Document(pmid=doc_row[0], pub_year=doc_row[1], title=doc_row[2], abstract=doc_row[3], origin=doc_row[4])
                        util.index_document(doc, index_update)

                    # update the data for each n-gram
                    for ngram in index_update:
                        shard = _ngram_to_shard_name(ngram)
                        self._idx_cursor.execute(f'''SELECT * FROM {shard} WHERE ngram = {PH}''', (ngram,))
                        rows = self._idx_cursor.fetchall()
                        updated_rows = []

                        for row in rows:
                            uuid = row[0]
                            ngram = row[1]
                            data = row[2]
                            size = row[3]
                            is_updated_row = False

                            decoded = msgpack.loads(data, strict_map_key=False)
                            if isinstance(decoded, list):
                                decoded = set(decoded)

                            for pmid in pmids_to_reindex:
                                if pmid in decoded:
                                    if isinstance(decoded, set):
                                        decoded.remove(pmid)
                                    else:
                                        del decoded[pmid]
                                    is_updated_row = True

                            if not is_updated_row:
                                continue

                            if isinstance(decoded, set):
                                decoded = list(decoded)
                            encoded = msgpack.dumps(decoded)

                            # the size is wrong but it could mess things up if we correct it
                            updated_rows.append((uuid, ngram, encoded, size))

                        # delete old version's data from the shard
                        self._idx_cursor.execute(f'''DELETE FROM {shard} WHERE uuid IN ({','.join(f'{PH}' for _ in updated_rows)})''', [row[0] for row in updated_rows])

                        # insert rows w/ updated data into the shard
                        for row in updated_rows:
                            self._idx_cursor.execute(f'''INSERT INTO {shard} (uuid, ngram, data, size) VALUES ({PH}, {PH}, {PH}, {PH})''', row)

                    # update document_versions table so there's no 'is_indexed' mismatch anymore
                    cursor = self._corpus.cursor()
                    for doc_row in batch:
                        pmid = doc_row[0]
                        previous_version = indexed_versions[pmid]
                        current_version = current_versions[pmid]
                        cursor.execute(f'''UPDATE document_versions SET is_indexed = 0 WHERE uuid = {PH}''', (previous_version,))
                        cursor.execute(f'''UPDATE document_versions SET is_indexed = 1 WHERE uuid = {PH}''', (current_version,))
                    self._corpus.commit()
                    cursor.close()

                self._index.commit()
                self._corpus.commit()
            

        ## --- defragment the shards ---
        fragmented_shards = self._get_tables("fragmented_%")
        fragmented_shards.sort()

        for fragmented_shard in fragmented_shards:
            self._defragment_shard(fragmented_shard)
            est_progress_time += est_time_defrag_per_shard
            progress = est_progress_time / est_total_time
            yield progress


        ## --- update disk cache ---
        self._idx_cursor.execute("SELECT term FROM query_cache")
        rows = self._idx_cursor.fetchall()
        cached_terms = [row[0] for row in rows]

        self._idx_cursor.execute('''DELETE FROM query_cache''')
        self._index.commit()

        self.prep_for_search(cached_terms)
        for cached_term in cached_terms:
            self.search_documents(cached_term)

            # we don't need to cache the term in memory, just on disk
            self.delete_term_from_memory(cached_term)

            est_progress_time += est_time_cache_refresh_per_term
            progress = est_progress_time / est_total_time
            yield progress

        progress = 1.0
        yield progress

    def get_document(self, pmid: int) -> Document | None:
        """Get a document from the corpus by PMID"""
        table_name = _pmid_to_table_name(pmid)
        self._corpus_cursor.execute(f'''SELECT pmid, pub_year, title, abstract, body, origin, citation_count FROM {table_name} WHERE pmid = {PH}''', (pmid,))
        row = self._corpus_cursor.fetchone()
        if row:
            return Document(
                pmid=row[0],
                pub_year=row[1],
                title=row[2],
                abstract=row[3],
                body=row[4],
                origin=row[5],
                citation_count=row[6]
            )
        return None

    def search_documents(self, term: str, start_year: int = MIN_CENSOR_YEAR, end_year: int = MAX_CENSOR_YEAR) -> set[int]:
        """Search the documents for a term, return all matching PMIDs"""
        # lowercase and remove punctuation but keep these chars: & | ( )
        term = util.sanitize_term_for_search(term)
        if not term:
            return set()

        # check term cache first
        if term in self._term_cache:
            return self._term_cache[term]
        
        # get subterms
        subterms = util.get_subterms(term)

        # handle errors
        if len(subterms) > 100:
            print(f"Too many subterms in composite term, maximum of 100 allowed: {term}")
            return set()
        if not subterms:
            print(f"No valid subterms found for: {term}")
            return set()

        # construct PMID sets for each subterm
        subterm_pmids = dict[str, set[int]]()
        for subterm in subterms:
            gram_n = util.get_ngram_n(subterm)

            # handle errors
            if gram_n > 20:
                print(f"Term is too long, maximum of 20-gram allowed: {subterm}")
                subterm_pmids[subterm] = set()
                continue

            # check disk cache for bigrams+
            if gram_n > 1:
                self._idx_cursor.execute(f'''SELECT data FROM query_cache WHERE term = {PH}''', (subterm,))
                row = self._idx_cursor.fetchone()
                if row:
                    data = row[0]
                    this_subterm_pmids = msgpack.loads(data, strict_map_key=False)

                    if isinstance(this_subterm_pmids, list):
                        this_subterm_pmids = set(this_subterm_pmids)

                    subterm_pmids[subterm] = this_subterm_pmids
                    continue

            # query the index to construct PMID set for the subterm
            this_subterm_pmids = self._search_index(subterm)
            subterm_pmids[subterm] = this_subterm_pmids

            # cache bigrams+ on disk
            if gram_n > 1:
                data = msgpack.dumps(list(this_subterm_pmids))
                cache_retries = 10
                for _ in range(cache_retries):
                    try:
                        if not gvars.POSTGRES_HOST:
                            self._idx_cursor.execute(f'''INSERT OR REPLACE INTO query_cache (term, data) VALUES ({PH}, {PH})''', (subterm, data))
                            self._index.commit()
                            break
                        # TODO: handle postgres case
                    except Exception as e:
                        print(f"Error caching query '{subterm}' to disk, retrying. Error message: {e}")
                        time.sleep(0.1)

        # construct PMID set for the term
        if len(subterms) == 1:
            term_pmids = subterm_pmids[subterms[0]]
        elif len(subterms) > 1:
            term_pmids = _evaluate_composite_term(term, subterm_pmids)

        # date-censor the PMID set
        if start_year > MIN_CENSOR_YEAR or end_year < MAX_CENSOR_YEAR:
            self.count_documents(start_year, end_year) # builds PMID set for this year range
            year_pmids = self._term_cache[(start_year, end_year)]
            term_pmids = term_pmids & year_pmids

        # save in memcache
        self._term_cache[term] = term_pmids
        return term_pmids

    def count_documents(self, start_year: int = MIN_CENSOR_YEAR, end_year: int = MAX_CENSOR_YEAR) -> int:
        """Count total documents in the index with optional year filtering"""
        if start_year <= MIN_CENSOR_YEAR and end_year >= MAX_CENSOR_YEAR:
            return self.n_documents

        # check cache first if the year range data is already computed
        year_tuple = (start_year, end_year)
        if year_tuple in self._term_cache:
            pmid_set = self._term_cache[year_tuple]
            return len(pmid_set)

        year_mask = (self.pmid_to_pub_year >= start_year) & (self.pmid_to_pub_year <= end_year)
        pmid_set = set(np.flatnonzero(year_mask).astype(int))
    
        # cache the pmid set
        self._term_cache[year_tuple] = pmid_set

        return len(pmid_set)

    def delete_all_documents(self) -> None:
        """Delete all documents in the index"""
        raise NotImplementedError("Delete not yet implemented.")
        # self._refresh_metadata()

    def is_indexing_in_progress(self) -> bool:
        fragmented_shards = self._get_tables("fragmented_%")
        if fragmented_shards:
            return True
        return False

    def prep_for_search(self, terms: list[str]) -> None:
        """Figure out which bi-grams are heavily used across unique subterms 
        and so might be worth caching in memory to avoid slow disk I/O"""
        self._bi_grams_maybe_should_cache.clear()
        self._ngram_cache.clear()
        self._term_cache.clear()

        unique_subterms = set[str]()
        for term in terms:
            sanitized = util.sanitize_term_for_search(term)
            subterms = util.get_subterms(sanitized)
            for subterm in subterms:
                gram_n = util.get_ngram_n(subterm)
                if gram_n < 2:
                    continue
                unique_subterms.add(subterm)
        
        for subterm in unique_subterms:
            bi_grams = util.get_ngrams(subterm, n=[2])
            for bi_gram in bi_grams:
                current_count = self._bi_grams_maybe_should_cache.get(bi_gram, 0)
                self._bi_grams_maybe_should_cache[bi_gram] = current_count + 1
        
        bi_grams = list(self._bi_grams_maybe_should_cache.keys())
        for bi_gram in bi_grams:
            if self._bi_grams_maybe_should_cache[bi_gram] < 20:
                del self._bi_grams_maybe_should_cache[bi_gram]

    def delete_term_from_memory(self, term: str) -> None:
        sanitized = util.sanitize_term_for_search(term)
        if sanitized in self._term_cache:
            del self._term_cache[sanitized]

    def top_n_pmids_by_year(self, pmids: set[int], top_n: int = 10) -> set[int]:
        """Given a set of PMIDs, return the top N PMIDs by publication year (most recent first). PMID is used as a tiebreaker."""
        if top_n < 1:
            return set()

        _sorted = sorted(pmids, key=lambda pmid: (self.pmid_to_pub_year[pmid], pmid), reverse=True)
        return _sorted[:top_n]
    
    def top_n_pmids_by_citation_count(self, pmids: set[int], top_n: int = 10) -> set[int]:
        """Given a set of PMIDs, return the top N PMIDs by citation count (highest first). PMID is used as a tiebreaker."""
        if top_n < 1:
            return set()

        if not self.pmid_to_citation_count.size:
            raise FastKmException("Citation counts not loaded, cannot sort by citation count")

        _sorted = sorted(pmids, key=lambda pmid: (self.pmid_to_citation_count[pmid], pmid), reverse=True)
        top_n = _sorted[:top_n]
        return top_n

    def top_n_pmids_by_impact_factor(self, pmids: set[int], top_n: int = 10) -> set[int]:
        """Given a set of PMIDs, return the top N PMIDs by impact factor (highest first). 
        Impact factor is defined as citation count / (current year - publication year + 0.5). 
        The 0.5 is added to avoid division by zero in all cases.
        PMID is used as a tiebreaker."""
        if top_n < 1:
            return set()

        if not self.pmid_to_citation_count.size:
            raise FastKmException("Citation counts not loaded, cannot sort by impact factor")

        current_year = datetime.datetime.now().year
        _sorted = sorted(pmids, key=lambda pmid: (self.pmid_to_citation_count[pmid] / (current_year - self.pmid_to_pub_year[pmid] + 0.5), pmid), reverse=True)
        return _sorted[:top_n]

    def close(self) -> None:
        """Close the index"""
        self._idx_cursor.close()
        self._corpus_cursor.close()
        self._index.close()
        self._corpus.close()

    def _get_db_connection(self, db_name: str):
        if gvars.POSTGRES_HOST:
            # create db
            conn = psycopg.connect(f"dbname=postgres user={gvars.POSTGRES_USER} password={gvars.POSTGRES_PASSWORD} host={gvars.POSTGRES_HOST} port={gvars.POSTGRES_PORT}")
            conn.autocommit = True
            with conn.cursor() as cur:
                try:
                    cur.execute(f"CREATE DATABASE {db_name}")
                except psycopg.errors.DuplicateDatabase:
                    pass
            conn.close()

            # connect to db
            conn = psycopg.connect(f"dbname={db_name} user={gvars.POSTGRES_USER} password={gvars.POSTGRES_PASSWORD} host={gvars.POSTGRES_HOST} port={gvars.POSTGRES_PORT}")
            # conn.autocommit = True
        else:
            os.makedirs(self.data_dir, exist_ok=True)
            conn = sqlite3.connect(os.path.join(self.data_dir, f"{db_name}.db"))
        return conn

    def _initialize(self) -> None:
        _using_sqlite = not gvars.POSTGRES_HOST
        _corpus_exists = os.path.exists(os.path.join(self.data_dir, "_corpus.db"))
        _index_exists = os.path.exists(os.path.join(self.data_dir, "_index.db"))
        if _using_sqlite and _corpus_exists and _index_exists:
            return

        corpus = self._get_db_connection("_corpus")
        cur = corpus.cursor()
        cur.execute(f'''CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value {BINARY_TYPE})''')
        cur.execute('''CREATE TABLE IF NOT EXISTS doc_origins (origin TEXT)''')
        cur.execute(f'''
                    CREATE TABLE IF NOT EXISTS document_versions (
                    uuid TEXT PRIMARY KEY, 
                    is_indexed INTEGER NOT NULL,
                    pmid INTEGER NOT NULL, 
                    pub_year INTEGER NOT NULL, 
                    title TEXT DEFAULT '',
                    abstract TEXT DEFAULT '',
                    body TEXT DEFAULT '',
                    origin TEXT DEFAULT '',
                    citation_count INTEGER DEFAULT 0
                )''')
        corpus.commit()
        cur.close()
        corpus.close()

        # create index db
        index = self._get_db_connection("_index")
        cur = index.cursor()
        cur.execute(f'''CREATE TABLE IF NOT EXISTS query_cache (term TEXT PRIMARY KEY, data {BINARY_TYPE})''')
        for i in range(1, 951):
            shard = f'shard_{i:04d}'
            cur.execute(f'''CREATE TABLE IF NOT EXISTS {shard} (uuid TEXT PRIMARY KEY, ngram TEXT, data {BINARY_TYPE}, size INTEGER)''')
        cur.execute(f'''CREATE INDEX IF NOT EXISTS idx_ngram_{shard} ON {shard} (ngram)''')
        index.commit()
        cur.close()
        index.close()

    def _load_metadata(self) -> None:
        """Load metadata about the corpus."""
        # load list of .xml files
        self._corpus_cursor.execute('''SELECT DISTINCT origin FROM doc_origins''')
        rows = self._corpus_cursor.fetchall()
        doc_origins = {row[0] for row in rows if row[0]}
        self.doc_origins = doc_origins
        
        # read pub years
        pmid_to_year_file = "pmid_to_year.npy"
        if os.path.exists(os.path.join(self.data_dir, pmid_to_year_file)):
            with open(os.path.join(self.data_dir, pmid_to_year_file), 'rb') as f:
                data_bytes = f.read()
                self.pmid_to_pub_year = np.frombuffer(data_bytes, dtype=np.uint16)

        # read citation counts
        pmid_to_citation_count_file = "pmid_to_citation_count.npy"
        if os.path.exists(os.path.join(self.data_dir, pmid_to_citation_count_file)):
            with open(os.path.join(self.data_dir, pmid_to_citation_count_file), 'rb') as f:
                data_bytes = f.read()
                self.pmid_to_citation_count = np.frombuffer(data_bytes, dtype=np.uint32)

        if self.pmid_to_pub_year.size:
            self.n_documents = np.count_nonzero(self.pmid_to_pub_year).item()

    def _refresh_metadata(self, new_docs: list[Document]) -> None:
        """Update the corpus database's metadata table."""
        # get max PMID so we can size the arrays.
        # we could use max(max(new_docs.pmid), len(self.pmid_to_pub_year) - 1),
        # but the new_docs don't necessarily have complete data. i.e., we might
        # have data for citation counts for documents that aren't in the corpus.
        # it's sort of an edge case but still.
        document_tables = self._get_tables("documents_%")
        document_tables.sort()

        max_pmid = 0
        for table in document_tables:
            self._corpus_cursor.execute(f'''SELECT MAX(pmid) FROM {table}''')
            row = self._corpus_cursor.fetchone()
            if row:
                max_table_pmid = row[0]
                max_pmid = max(max_pmid, max_table_pmid)

        # create the arrays
        _pmid_to_pub_year = np.zeros(max_pmid + 1, dtype=np.uint16)
        _pmid_to_citation_count = np.zeros(max_pmid + 1, dtype=np.uint32)

        # populate the arrays with existing data if available
        if self.pmid_to_pub_year.size:
            _pmid_to_pub_year[:self.pmid_to_pub_year.size] = self.pmid_to_pub_year
        if self.pmid_to_citation_count.size:
            _pmid_to_citation_count[:self.pmid_to_citation_count.size] = self.pmid_to_citation_count

        # add new data from the documents
        for doc in new_docs:
            pmid = doc.pmid

            if pmid < 1 or pmid >= _pmid_to_pub_year.size:
                continue

            existing_pub_year = _pmid_to_pub_year[pmid]
            existing_citation_count = _pmid_to_citation_count[pmid]

            pub_year = doc.pub_year if doc.pub_year is not None else existing_pub_year
            citation_count = doc.citation_count if doc.citation_count is not None else existing_citation_count

            if pub_year > MAX_CENSOR_YEAR or pub_year < MIN_CENSOR_YEAR:
                print(f"Warning: invalid publication year {pub_year} for PMID {pmid}, setting to {MAX_CENSOR_YEAR}")
                pub_year = MAX_CENSOR_YEAR
            if citation_count > 1_000_000_000 or citation_count < 0:
                print(f"Warning: invalid citation count {citation_count} for PMID {pmid}, setting to 0")
                citation_count = 0

            _pmid_to_pub_year[pmid] = np.uint16(pub_year)
            _pmid_to_citation_count[pmid] = np.uint32(citation_count)

        # save to disk
        pmid_to_year_file = "pmid_to_year.npy"
        packed_pub_years = np.ndarray.tobytes(_pmid_to_pub_year)
        with open(os.path.join(self.data_dir, pmid_to_year_file), 'wb') as f:
            f.write(packed_pub_years)

        pmid_to_citation_count_file = "pmid_to_citation_count.npy"
        packed_citation_counts = np.ndarray.tobytes(_pmid_to_citation_count)
        with open(os.path.join(self.data_dir, pmid_to_citation_count_file), 'wb') as f:
            f.write(packed_citation_counts)

        # load the new metadata
        self._load_metadata()

    def _get_tables(self, like: str) -> list[str]:
        # a little hacky but figure out what database we're querying
        if "doc" in like:
            cursor = self._corpus_cursor
        elif "shard" in like or "fragmented" in like:
            cursor = self._idx_cursor
        else:
            raise FastKmException(f"Unknown table type for like pattern: {like}")

        # fetch table names
        if not gvars.POSTGRES_HOST:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{like}'")
        else:
            cursor.execute(f"SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE '{like}'")

        rows = cursor.fetchall()
        tables = [row[0] for row in rows]
        tables.sort()
        return tables

    def _search_index(self, subterm: str) -> set[int]:
        gram_n = util.get_ngram_n(subterm)

        # split into ngrams
        ngrams_to_data = dict()
        
        if gram_n <= 2:
            ngrams_to_data[subterm] = None # could be either set or dict later
        else:
            tokens = subterm.split(" ")
            bi_grams = []
            for i in range(len(tokens) - 1):
                bi_gram = f"{tokens[i]} {tokens[i+1]}"
                ngrams_to_data[bi_gram] = dict()
                bi_grams.append(bi_gram)

        # retrieve ngram data from database
        for ngram in ngrams_to_data:
            if ngram in self._ngram_cache:
                ngrams_to_data[ngram] = self._ngram_cache[ngram]
                continue

            shard_name = _ngram_to_shard_name(ngram)
            self._idx_cursor.execute(f'''SELECT data FROM {shard_name} WHERE ngram = {PH}''', (ngram,))
            rows = self._idx_cursor.fetchall()

            if not rows:
                # ngram does not occur in any document
                ngrams_to_data[ngram] = set() if gram_n <= 2 else dict()
                continue

            for row in rows:
                data = row[0]
                pmids_maybe_with_positions = msgpack.loads(data, strict_map_key=False)

                if isinstance(pmids_maybe_with_positions, list):
                    pmids_maybe_with_positions = set(pmids_maybe_with_positions)

                if ngram not in ngrams_to_data or ngrams_to_data[ngram] is None:
                    ngrams_to_data[ngram] = pmids_maybe_with_positions
                else:
                    ngrams_to_data[ngram].update(pmids_maybe_with_positions)

            # cache expensive ngram data in memory if it's going to be used a lot
            if len(ngrams_to_data[ngram]) > 1_000_000 and ngram in self._bi_grams_maybe_should_cache:
                self._ngram_cache[ngram] = ngrams_to_data[ngram]

        # put ngrams together if needed
        if gram_n <= 2:
            if isinstance(ngrams_to_data[subterm], set):
                pmids = ngrams_to_data[subterm]
            else:
                pmids = set(ngrams_to_data[subterm])
        else:
            pmids = set()
            first_bi_gram = bi_grams[0]
            first_bi_gram_pmid_to_positions: dict = ngrams_to_data[first_bi_gram]
            maybe_pmids = set(first_bi_gram_pmid_to_positions)
            
            for bi_gram in bi_grams[1:]:
                maybe_pmids.intersection_update(ngrams_to_data[bi_gram])

            for pmid in maybe_pmids:
                for pos0 in first_bi_gram_pmid_to_positions[pmid]:
                    match = True
                    current_pos = pos0
                    for bi_gram in bi_grams[1:]:
                        current_pos += 1
                        bi_gram_pmid_to_positions = ngrams_to_data[bi_gram]
                        if current_pos not in bi_gram_pmid_to_positions[pmid]:
                            match = False
                            break
                    if match:
                        pmids.add(pmid)
                        break

        return pmids

    def _defragment_shard(self, fragmented_shard: str) -> None:
        # start_time = time.perf_counter()
        shard = fragmented_shard.replace('fragmented_', '')
        fragment_cursor = self._index.cursor()
        shard_cursor = self._index.cursor()
        max_row_bytes = 1_000_000
        max_batch_bytes = 50 * max_row_bytes
        max_batch_terms = 100

        fragment_cursor.execute(f'''SELECT ngram, data FROM {fragmented_shard} ORDER BY ngram''')
        ngram_to_fragment_rows = dict[str, list[tuple[str, str, bytes, int]]]()
        batch_bytes = 0
        while True:
            # get fragmented shard data and add it to the dict
            fragmented_shard_rows = fragment_cursor.fetchmany(max_batch_terms)
            if not fragmented_shard_rows:
                break

            for ngram, data in fragmented_shard_rows:
                if ngram not in ngram_to_fragment_rows:
                    ngram_to_fragment_rows[ngram] = []
                ngram_to_fragment_rows[ngram].append(("", ngram, data, len(data)))
                batch_bytes += len(data)

            if len(ngram_to_fragment_rows) < max_batch_terms and batch_bytes < max_batch_bytes and len(fragmented_shard_rows) == max_batch_terms:
                continue

            # get (non-fragmented) shard data and add it to the dict
            ngrams = list(ngram_to_fragment_rows.keys())
            shard_cursor.execute(f'''SELECT * FROM {shard} WHERE ngram IN ({','.join(f'{PH}' for _ in ngrams)}) AND size < {max_row_bytes}''', tuple(ngrams))
            shard_rows = shard_cursor.fetchall()
            for row in shard_rows:
                ngram = row[1]
                ngram_to_fragment_rows[ngram].append(row)

            # combine fragments
            defragmented_rows = []
            for i, ngram in enumerate(ngram_to_fragment_rows):
                fragment_rows = ngram_to_fragment_rows[ngram]
                fragment_rows.reverse() # reverse the list so the existing shard data (if any) is first in the list
                gram_n = util.get_ngram_n(ngram)
                existing_uuid = fragment_rows[0][0] # empty string if there is no existing shard data for this term
                
                defragmented_row_data = set() if gram_n == 1 else dict()
                defragmented_row_size = 0
                defragmented_row_uuid = str(uuid.uuid4()) if not existing_uuid else existing_uuid
                for i, fragment_row in enumerate(fragment_rows):
                    encoded_fragment = fragment_row[2]
                    decoded_fragment = msgpack.loads(encoded_fragment, strict_map_key=False)
                    defragmented_row_data.update(decoded_fragment)
                    defragmented_row_size += len(encoded_fragment)

                    if defragmented_row_size > max_row_bytes or i == len(fragment_rows) - 1:
                        # msgpack cannot serialize sets, convert to list
                        if isinstance(defragmented_row_data, set):
                            defragmented_row_data = list(defragmented_row_data)
                        defragmented_row_data_bytes = msgpack.dumps(defragmented_row_data) 

                        # defragmented_row_size is technically wrong here (too high). but if we insert two rows with size <1_000_000
                        # into the shard, that will mess everything up.
                        defragmented_rows.append((defragmented_row_uuid, ngram, defragmented_row_data_bytes, defragmented_row_size))

                        # reset for next row (really only needed if defragmented_row_size > 1_000_000)
                        defragmented_row_data = set() if gram_n == 1 else dict()
                        defragmented_row_uuid = str(uuid.uuid4())
                        defragmented_row_size = 0

            # insert into shard table
            shard_cursor.executemany(f'''INSERT INTO {shard} (uuid, ngram, data, size) VALUES ({PH}, {PH}, {PH}, {PH}) ON CONFLICT (uuid) DO UPDATE SET ngram = EXCLUDED.ngram, data = EXCLUDED.data, size = EXCLUDED.size''', defragmented_rows)

            # reset for next batch
            ngram_to_fragment_rows.clear()
            batch_bytes = 0

        # commit the inserts to the shard
        self._index.commit()

        # drop the fragmented shard table
        self._index.execute(f'''DROP TABLE IF EXISTS {fragmented_shard}''')
        self._index.commit()

        # close cursors
        fragment_cursor.close()
        shard_cursor.close()

        # duration = time.perf_counter() - start_time
        # print(f"Defragmented shard {fragmented_shard} in {duration:.2f} seconds")

def _ngram_to_shard_name(ngram: str, get_fragmented_name: bool = False) -> str:
    # sqlite has a max num tables of 2000, so if we want 2 tables per shard,
    # (one fragmented and one not), we can have up to 1000 shards.
    # we will use 950 shards to leave some room for other tables (e.g. 
    # metadata, pmid counts, etc.)
    n_total_shards = 950

    # could use md5 or sha256, this is kind of ugly
    hash_val = zlib.crc32(ngram.encode('utf-8')) & 0xffffffff
    shard_num = (hash_val % n_total_shards) + 1
    shard_name = f'shard_{shard_num:04d}'

    if get_fragmented_name:
        return f'fragmented_{shard_name}'
    
    return shard_name

def _pmid_to_table_name(pmid: int) -> str:
    return f'documents_{(pmid // 1_000_000):02d}m'

def _evaluate_composite_term(composite_term: str, pmids: dict[str, set[int]]) -> set[int]:
    namespace = dict()

    subterms = util.get_subterms(composite_term, keep_logical_operators=True)
    for i in range(len(subterms)):
        subterm = subterms[i]

        # don't modify logical operators
        if subterm in util.logical_operators:
            continue

        # we need to make the subterm a valid Python variable name
        # 1. replace spaces with underscores
        # 2. put an underscore in front, in case the subterm starts with a number
        sanitized_subterm = "_" + subterm.replace(" ", "_")
        
        # add the subterm to eval namespace
        namespace[sanitized_subterm] = pmids.get(subterm, set())
        subterms[i] = sanitized_subterm
    
    space_replaced_composite_term = " ".join(subterms)
    try:
        return eval(space_replaced_composite_term, {"__builtins__": {}}, namespace)
    except Exception as e:
        # raise FastKmException(f"Malformed expression: {composite_term}")
        print(f"Error evaluating composite term '{composite_term}': {e}")
        return set()