import os
import time
import datetime
from typing import Generator
import uuid
import zlib
import sqlite3
import msgpack
import numpy as np
from src.fast_km_exception import FastKmException
from src.documents.document import Document
import src.indexing.indexing_util as util
import src.global_vars as gvars

MIN_CENSOR_YEAR = gvars.MIN_CENSOR_YEAR
MAX_CENSOR_YEAR = gvars.MAX_CENSOR_YEAR

"""
PubMed Abstract Indexing System
================================================
This overview was written by Claude Opus 4.1 and revised by a human for accuracy.

This class implements an inverted index for searching ~40 million PubMed abstracts 
with the goal of achieving:
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
        cursor = self._corpus.cursor()

        # create document tables if they don't exist
        doc_tables = set(_pmid_to_table_name(doc.pmid) for doc in documents)
        for doc_table in doc_tables:
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

        # add or update documents
        for new_doc in documents:
            version_uuid = str(uuid.uuid4())
            is_indexed = 0
            doc_table = _pmid_to_table_name(new_doc.pmid)

            # special case. if we're just updating a non-indexed field, just update that field. no need to create a new doc.
            if new_doc.title is None and new_doc.abstract is None and new_doc.body is None:
                if new_doc.citation_count is not None:
                    cursor.execute(f'''UPDATE {doc_table} SET citation_count = ? WHERE pmid = ?''', (new_doc.citation_count, new_doc.pmid))
                if new_doc.pub_year is not None:
                    cursor.execute(f'''UPDATE {doc_table} SET pub_year = ? WHERE pmid = ?''', (new_doc.pub_year, new_doc.pmid))
                if new_doc.origin is not None:
                    cursor.execute(f'''UPDATE {doc_table} SET origin = ? WHERE pmid = ?''', (new_doc.origin, new_doc.pmid))
                continue

            new_doc_row = (
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
            try:
                # add doc to the corpus
                cursor.execute(f'''
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
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', new_doc_row)
                
            except sqlite3.IntegrityError as e:
                if 'PRIMARY KEY constraint failed' not in str(e) and 'UNIQUE constraint failed' not in str(e):
                    raise e
                
                # get current canonical version
                cursor.execute(f'''SELECT * FROM {doc_table} WHERE pmid = ?''', (new_doc.pmid,))
                current_doc_row = cursor.fetchone()
                
                updated_doc_row = (
                    version_uuid, 
                    is_indexed, 
                    new_doc.pmid, 
                    new_doc.pub_year, 
                    new_doc.title if new_doc.title is not None else current_doc_row[4], 
                    new_doc.abstract if new_doc.abstract is not None else current_doc_row[5], 
                    new_doc.body if new_doc.body is not None else current_doc_row[6], 
                    new_doc.origin if new_doc.origin is not None else current_doc_row[7], 
                    new_doc.citation_count if new_doc.citation_count is not None else current_doc_row[8]
                )

                # if the updated text/year/citations are identical to the current doc, skip.
                # this helps when the baseline year changes. no need to re-index a doc if nothing changed.
                if (updated_doc_row[3] == current_doc_row[3] and # same pub_year
                    updated_doc_row[4] == current_doc_row[4] and # same title
                    updated_doc_row[5] == current_doc_row[5] and # same abstract
                    updated_doc_row[6] == current_doc_row[6] and # same body
                    updated_doc_row[8] == current_doc_row[8]     # same citation_count
                ):
                    continue
                
                # replace the current version but save both current+new versions to the document_versions table.
                doc_versions = [updated_doc_row, current_doc_row]
                cursor.executemany('''
                                    INSERT OR REPLACE INTO document_versions (
                                        uuid, 
                                        is_indexed, 
                                        pmid, 
                                        pub_year, 
                                        title, 
                                        abstract,
                                        body,
                                        origin,
                                        citation_count
                                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', doc_versions)
                
                # put the new version in the document table
                cursor.execute(f'''
                                INSERT OR REPLACE INTO {doc_table} (
                                    uuid, 
                                    is_indexed, 
                                    pmid, 
                                    pub_year, 
                                    title, 
                                    abstract,
                                    body,
                                    origin,
                                    citation_count
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', updated_doc_row)

        # update the doc_origins table with new origin name(s) - likely just one .xml file
        origins = {abstract.origin for abstract in documents if abstract.origin}
        if origins:
            cursor.executemany(f'''INSERT INTO doc_origins (origin) VALUES (?)''', [(origin,) for origin in origins])

        # commit changes
        self._corpus.commit()

        # update metadata (commits)
        self._refresh_metadata(documents)
        cursor.close()

    def index_documents(self) -> Generator[float, None, None]:
        print("Starting indexing...")
        yield 0.0

        ## --- index the documents (builds the fragmented shards) ---

        # count number of documents to be indexed so we can calculate progress
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

        if n_docs_need_indexing > 0:
            print("Building fragmented shards...")
        else:
            print("No documents to index, skipping building fragmented shards.")

        docs_indexed = 0
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
                    self._idx_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {fragmented_shard} (ngram TEXT, data BLOB)''')

                    for ngram in ngrams:
                        data_fragment = index_update[ngram]
                        data_fragment_encoded = _msgpack_dump(data_fragment)
                        self._idx_cursor.execute(f'''INSERT INTO {fragmented_shard} (ngram, data) VALUES (?, ?)''', (ngram, data_fragment_encoded))

                # mark the documents as indexed
                update_cursor = self._corpus.cursor()
                for doc_row in batch:
                    pmid = doc_row[0]
                    update_cursor.execute(f'''UPDATE {doc_table} SET is_indexed = 1 WHERE pmid = ?''', (pmid,))

                # commit the batch
                self._index.commit()
                self._corpus.commit()
                update_cursor.close()

                # calculate/report progress.
                # this is step 1 of 4, so max progress is 25% for this step.
                docs_indexed += len(batch)
                progress = (docs_indexed / n_docs_need_indexing) * 0.25
                yield progress

        # step 1 of 4 complete
        progress = 0.25
        yield progress

        ## --- manage document version conflicts ---
        print("Checking for version conflicts...")
        # get the indexed version UUIDs of all docs that have >1 version
        version_cursor = self._corpus.cursor()
        version_cursor.execute('''SELECT pmid, uuid FROM document_versions WHERE is_indexed = 1''')
        
        pmids_to_reindex = dict[int, list[str]]() # pmid -> list of n-grams that need to be re-indexed for this doc
        indexed_versions = dict[int, str]() # pmid -> uuid, for the version that is currently indexed in the defragmented shard
        canonical_versions = dict[int, str]() # pmid -> uuid, for the version that is in the document corpus (the 'canonical' version)

        while True:
            # get the indexed version UUID
            indexed_version_row = version_cursor.fetchone()
            if not indexed_version_row:
                break
            pmid = indexed_version_row[0]
            indexed_version_uuid = indexed_version_row[1]

            # get the canonical version UUID
            doc_table = _pmid_to_table_name(pmid)
            self._corpus_cursor.execute(f'''SELECT pmid, uuid FROM {doc_table} WHERE pmid = ?''', (pmid,))
            canonical_version_row = self._corpus_cursor.fetchone()
            canonical_version_uuid = canonical_version_row[1]

            # determine if there's a version conflict
            if indexed_version_uuid != canonical_version_uuid:
                pmids_to_reindex[pmid] = []
                indexed_versions[pmid] = indexed_version_uuid
                canonical_versions[pmid] = canonical_version_uuid
        
        version_cursor.close()
                
        if pmids_to_reindex:
            print(f"Found {len(pmids_to_reindex)} documents with version conflicts to resolve.")
        else:
            print("No version conflicts found.")

        # if an abstract has been updated, we need to delete the old version's data from the index.
        # in these cases, there will be a mismatch between the 'is_indexed' columns in the 'documents' and 'document_versions' tables.
        # the 'is_indexed' column in document_versions will be 1 for the old version but the UUID will not match the document in the 'documents' table.
        # this is not the clearest situation, I'd like to make it more explicit at some point.


        count_done = 0
        ngrams_indexed = dict[str, dict[int, list[int]]]()    # saves memory
        ngrams_canonical = dict[str, dict[int, list[int]]]()  # saves memory
        cursor = self._corpus.cursor()
        for pmid in pmids_to_reindex.keys():
            ngrams_indexed.clear()
            ngrams_canonical.clear()

            # retrieve the indexed version (by uuid, primary key)
            indexed_uuid = indexed_versions[pmid]
            self._corpus_cursor.execute(f'''SELECT pmid, uuid, title, abstract FROM document_versions WHERE uuid = ?''', (indexed_uuid,))
            indexed_version = self._corpus_cursor.fetchone()
            indexed_doc = Document(pmid=pmid, pub_year=0, title=indexed_version[2], abstract=indexed_version[3], origin='')
            util.index_document(indexed_doc, ngrams_indexed)

            # retrieve the canonical version (by pmid, primary key)
            doc_table = _pmid_to_table_name(pmid)
            self._corpus_cursor.execute(f'''SELECT pmid, uuid, title, abstract FROM {doc_table} WHERE pmid = ?''', (pmid,))
            canonical_version = self._corpus_cursor.fetchone()
            canonical_doc = Document(pmid=pmid, pub_year=0, title=canonical_version[2], abstract=canonical_version[3], origin='')
            util.index_document(canonical_doc, ngrams_canonical)

            # determine if any n-grams have been deleted in the canonical version
            deleted_ngrams = set[str]()
            for ngram in ngrams_indexed:
                if ngram not in ngrams_canonical:
                    deleted_ngrams.add(ngram)

            for ngram in deleted_ngrams:
                # delete the data from the index for this n-gram
                shard = _ngram_to_shard_name(ngram)
                self._idx_cursor.execute(f'''SELECT * FROM {shard} WHERE ngram = ?''', (ngram,))
                rows = self._idx_cursor.fetchall()

                for row in rows:
                    uuid = row[0]
                    ngram = row[1]
                    data = row[2]
                    size = row[3]
                    is_updated_row = False

                    decoded = _msgpack_load(data)

                    if pmid in decoded:
                        if isinstance(decoded, set):
                            decoded.remove(pmid)
                        else:
                            del decoded[pmid]
                        is_updated_row = True

                    if not is_updated_row:
                        continue

                    encoded = _msgpack_dump(decoded)
                    self._idx_cursor.execute(f'''UPDATE {shard} SET data = ? WHERE uuid = ?''', (encoded, uuid))

            # update document_versions table so there's no 'is_indexed' mismatch anymore
            previous_version_uuid = indexed_versions[pmid]
            canonical_version_uuid = canonical_versions[pmid]
            cursor.execute(f'''UPDATE document_versions SET is_indexed = 0 WHERE uuid = ?''', (previous_version_uuid,))
            cursor.execute(f'''UPDATE document_versions SET is_indexed = 1 WHERE uuid = ?''', (canonical_version_uuid,))

            # calculate progress for step 2
            count_done += 1
            progress = count_done / len(pmids_to_reindex) * 0.25 + 0.25 
            yield progress

            if count_done % 1000 == 0:
                self._corpus.commit()
                self._index.commit()

        self._corpus.commit()
        self._index.commit()
        cursor.close()

        # step 2 of 4 complete
        progress = 0.5
        yield progress

        ## --- defragment the shards ---
        fragmented_shards = self._get_tables("fragmented_%")
        fragmented_shards.sort()

        if fragmented_shards:
            print("Defragmenting shards...")
        else:
            print("No shards to defragment.")

        n_defragmented = 0
        for fragmented_shard in fragmented_shards:
            self._defragment_shard(fragmented_shard)

            # calculate progress for step 3
            n_defragmented += 1
            progress = (n_defragmented / len(fragmented_shards)) * 0.25 + 0.5
            yield progress


        # step 3 of 4 complete
        progress = 0.75
        yield progress


        ## --- update disk cache ---

        # count terms in the disk cache, this is used to estimate how long the cache refresh will take
        self._idx_cursor.execute('''SELECT COUNT(term) FROM query_cache''')
        rows = self._idx_cursor.fetchone()
        cached_term_count = rows[0] if rows else 0

        if cached_term_count > 0:
            print("Refreshing query cache...")
        else:
            print("No terms in query cache, skipping cache refresh.")

        self._idx_cursor.execute("SELECT term FROM query_cache")
        rows = self._idx_cursor.fetchall()
        cached_terms = [row[0] for row in rows]

        self._idx_cursor.execute('''DELETE FROM query_cache''')
        self._index.commit()

        self.prep_for_search(cached_terms)
        terms_refreshed = 0
        for cached_term in cached_terms:
            self.search_documents(cached_term)

            # we don't need to cache the term in memory, just on disk
            self.delete_term_from_memory(cached_term)

            terms_refreshed += 1
            progress = (terms_refreshed / cached_term_count) * 0.25 + 0.75
            yield progress

        # step 4 of 4 complete
        print("Indexing complete.")
        progress = 1.0
        yield progress

    def get_document(self, pmid: int) -> Document | None:
        """Get a document from the corpus by PMID"""
        table_name = _pmid_to_table_name(pmid)
        self._corpus_cursor.execute(f'''SELECT pmid, pub_year, title, abstract, body, origin, citation_count FROM {table_name} WHERE pmid = ?''', (pmid,))
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
                self._idx_cursor.execute(f'''SELECT data FROM query_cache WHERE term = ?''', (subterm,))
                row = self._idx_cursor.fetchone()
                if row:
                    data = row[0]
                    this_subterm_pmids = _msgpack_load(data)
                    subterm_pmids[subterm] = this_subterm_pmids
                    continue

            # query the index to construct PMID set for the subterm
            this_subterm_pmids = self._search_index(subterm)
            subterm_pmids[subterm] = this_subterm_pmids

            # cache bigrams+ on disk
            if gram_n > 1:
                data = _msgpack_dump(this_subterm_pmids)
                cache_retries = 10
                for _ in range(cache_retries):
                    try:
                        self._idx_cursor.execute(f'''INSERT OR REPLACE INTO query_cache (term, data) VALUES (?, ?)''', (subterm, data))
                        self._index.commit()
                        break
                    except Exception as e:
                        print(f"Error caching query '{subterm}' to disk, retrying. Error message: {e}")
                        time.sleep(0.1)

        # construct PMID set for the term
        if len(subterms) == 1:
            term_pmids = subterm_pmids[subterms[0]]
        elif len(subterms) > 1:
            term_pmids = _evaluate_composite_term(term, subterm_pmids)

        # date-censor the PMID set
        term_pmids = self.date_censor_pmids(term_pmids, start_year, end_year)

        # save in memcache
        self._term_cache[term] = term_pmids
        return term_pmids
    
    def date_censor_pmids(self, pmids: set[int], start_year: int = MIN_CENSOR_YEAR, end_year: int = MAX_CENSOR_YEAR) -> set[int]:
        """Given a set of PMIDs, return the subset that fall within the specified year range"""
        if start_year <= MIN_CENSOR_YEAR and end_year >= MAX_CENSOR_YEAR:
            return pmids

        self.count_documents(start_year, end_year) # builds PMID set for this year range
        year_pmids = self._term_cache[(start_year, end_year)]
        censored_pmids = (pmids & year_pmids)
        censored_pmids = set(int(pmid) for pmid in censored_pmids)
        return censored_pmids

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
        os.makedirs(self.data_dir, exist_ok=True)
        conn = sqlite3.connect(os.path.join(self.data_dir, f"{db_name}.db"))

        # use WAL and full synchronous for better corruption resistance
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=FULL;')

        return conn

    def _initialize(self) -> None:
        _corpus_exists = os.path.exists(os.path.join(self.data_dir, "_corpus.db"))
        _index_exists = os.path.exists(os.path.join(self.data_dir, "_index.db"))

        # check that the shards have an index on ngram
        if _index_exists:
            index = self._get_db_connection("_index")
            cur = index.cursor()
            cur.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'shard_%' ''')
            shard_tables = [row[0] for row in cur.fetchall()]
            for shard in shard_tables:
                cur.execute(f'''PRAGMA index_list({shard})''')
                indexes = [row[1] for row in cur.fetchall()]
                if f'idx_ngram_{shard}' not in indexes:
                    print(f"Ngram column index is missing from shard {shard}, creating it now.")

                    # index the ngram column
                    cur.execute(f'''CREATE INDEX IF NOT EXISTS idx_ngram_{shard} ON {shard} (ngram)''')
                    index.commit()

            cur.close()
            index.close()

        if _corpus_exists and _index_exists:
            return

        corpus = self._get_db_connection("_corpus")
        cur = corpus.cursor()
        cur.execute(f'''CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value BLOB)''')
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
        cur.execute(f'''CREATE TABLE IF NOT EXISTS query_cache (term TEXT PRIMARY KEY, data BLOB)''')
        for i in range(1, 951):
            shard = f'shard_{i:04d}'
            cur.execute(f'''CREATE TABLE IF NOT EXISTS {shard} (uuid TEXT PRIMARY KEY, ngram TEXT, data BLOB, size INTEGER)''')
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
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{like}'")

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
            self._idx_cursor.execute(f'''SELECT data FROM {shard_name} WHERE ngram = ?''', (ngram,))
            rows = self._idx_cursor.fetchall()

            if not rows:
                # ngram does not occur in any document
                ngrams_to_data[ngram] = set() if gram_n <= 2 else dict()
                continue

            for row in rows:
                data = row[0]
                pmids_maybe_with_positions = _msgpack_load(data)
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
        shard = fragmented_shard.replace('fragmented_', '')
        fragment_cursor = self._index.cursor()
        shard_cursor = self._index.cursor()
        max_row_bytes = 1_000_000
        
        from itertools import groupby

        # get fragmented shard data and group by ngram
        fragment_cursor.execute(f'''SELECT ngram, data FROM {fragmented_shard} ORDER BY ngram''')
        for ngram, rows in groupby(fragment_cursor, key=lambda row: row[0]):
            fragment_rows = [("", ngram, data, len(data)) for _, data in rows]
            
            # get existing shard rows for this ngram
            shard_cursor.execute(f'''SELECT * FROM {shard} WHERE ngram = ?''', (ngram,))
            shard_rows = shard_cursor.fetchall()

            # get the shard row to add to, if any
            # shard table structure: uuid TEXT PRIMARY KEY, ngram TEXT, data BLOB, size INTEGER
            active_shard_row = [shard_row for shard_row in shard_rows if shard_row[3] < max_row_bytes]
            active_shard_row = active_shard_row[0] if active_shard_row else None

            # insert the active shard row so it gets combined with the fragments
            if active_shard_row:
                fragment_rows.insert(0, active_shard_row)

            # combine fragments
            gram_n = util.get_ngram_n(ngram)
            existing_uuid = fragment_rows[0][0] # empty string if there is no existing shard data for this term
            
            defragmented_rows = []
            defragmented_row_data = set() if gram_n == 1 else dict()
            defragmented_row_size = 0
            defragmented_row_uuid = str(uuid.uuid4()) if not existing_uuid else existing_uuid
            for i, fragment_row in enumerate(fragment_rows):
                encoded_fragment = fragment_row[2]
                decoded_fragment = _msgpack_load(encoded_fragment)
                defragmented_row_data.update(decoded_fragment)
                defragmented_row_size += len(encoded_fragment)

                if defragmented_row_size > max_row_bytes or i == len(fragment_rows) - 1:
                    defragmented_row_data_bytes = _msgpack_dump(defragmented_row_data) 

                    # defragmented_row_size is technically wrong here (too high). but if we insert two rows with size <1_000_000
                    # into the shard, that will mess everything up.
                    defragmented_rows.append((defragmented_row_uuid, ngram, defragmented_row_data_bytes, defragmented_row_size))

                if defragmented_row_size > max_row_bytes:
                    # reset for next row
                    defragmented_row_data = set() if gram_n == 1 else dict()
                    defragmented_row_size = 0
                    defragmented_row_uuid = str(uuid.uuid4())

            # insert into shard table
            shard_cursor.executemany(f'''INSERT INTO {shard} (uuid, ngram, data, size) VALUES (?, ?, ?, ?) ON CONFLICT (uuid) DO UPDATE SET ngram = EXCLUDED.ngram, data = EXCLUDED.data, size = EXCLUDED.size''', defragmented_rows)



            # --- data integrity check for ngrams with >1 defragmented row (rare) ---
            shard_cursor.execute(f'''SELECT COUNT(*) FROM {shard} WHERE ngram = ?''', (ngram,))
            row = shard_cursor.fetchone()
            n_defragmented_rows = row[0] if row else 0
            if n_defragmented_rows <= 1:
                continue

            # each PMID should only be observed once.
            # if it is observed >1 time, we need to figure out which version is correct.
            pmid_counts = dict[int, int]()
            shard_cursor.execute(f'''SELECT * FROM {shard} WHERE ngram = ?''', (ngram,))
            rows = shard_cursor.fetchall()
            for row in rows:
                data = row[2]
                decoded = _msgpack_load(data)

                for pmid in decoded:
                    if pmid not in pmid_counts:
                        pmid_counts[pmid] = 0
                    pmid_counts[pmid] += 1

            duplicate_pmids = [pmid for pmid, count in pmid_counts.items() if count > 1]
            if not duplicate_pmids:
                # data integrity for this n-gram is OK, no duplicate PMIDs found
                continue

            # delete the duplicate PMID data from all rows for this n-gram
            for row in rows:
                _uuid = row[0]
                ngram = row[1]
                data = row[2]
                size = row[3]

                decoded = _msgpack_load(data)
                is_updated_row = False

                for pmid in duplicate_pmids:
                    if pmid in decoded:
                        if isinstance(decoded, set):
                            decoded.remove(pmid)
                        else:
                            del decoded[pmid]
                        is_updated_row = True

                if not is_updated_row:
                    continue

                encoded = _msgpack_dump(decoded)
                shard_cursor.execute(f'''UPDATE {shard} SET data = ? WHERE uuid = ?''', (encoded, _uuid))
            
            # fetch a shard row for this ngram (doesn't really matter which row)
            shard_cursor.execute(f'''SELECT * FROM {shard} WHERE ngram = ?''', (ngram,))
            row = shard_cursor.fetchone()
            _uuid = row[0]
            data = row[2]
            decoded = _msgpack_load(data)

            # re-index these PMIDs so the data is correct
            temp_index = dict()
            for pmid in duplicate_pmids:
                temp_index.clear()

                # get the canonical version of this document and re-index it
                doc_table = _pmid_to_table_name(pmid)
                self._corpus_cursor.execute(f'''SELECT title, abstract FROM {doc_table} WHERE pmid = ?''', (pmid,))
                doc_row = self._corpus_cursor.fetchone()
                indexed_doc = Document(pmid=pmid, pub_year=0, title=doc_row[0], abstract=doc_row[1], origin='')
                util.index_document(indexed_doc, temp_index)

                # update the shard row with correct data
                correct_ngram_data = temp_index.get(ngram, set() if util.get_ngram_n(ngram) == 1 else dict())
                decoded.update(correct_ngram_data)
            
            encoded = _msgpack_dump(decoded)
            shard_cursor.execute(f'''UPDATE {shard} SET data = ? WHERE uuid = ?''', (encoded, _uuid))
            

        # commit the changes to the shard
        self._index.commit()

        # drop the fragmented shard table
        self._index.execute(f'''DROP TABLE IF EXISTS {fragmented_shard}''')
        self._index.commit()

        # close cursors
        fragment_cursor.close()
        shard_cursor.close()

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
    
def _msgpack_load(data: bytes) -> set[int] | dict[int, list[int]]:
    decoded = msgpack.loads(data, strict_map_key=False)
    if isinstance(decoded, list):
        return set(decoded)
    return decoded

def _msgpack_dump(data: set[int] | dict[int, list[int]]) -> bytes:
    if isinstance(data, set):
        data = list(data)
    return msgpack.dumps(data)