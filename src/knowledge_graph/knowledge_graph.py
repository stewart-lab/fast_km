import os
import sqlite3
import json
from src.indexing.index import Index
from src.indexing.indexing_util import sanitize_term_for_search
from src.knowledge_graph.params import RelationshipModel

class KnowledgeGraph:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.cache = dict()

        _kg_exists = os.path.exists(os.path.join(self.data_dir, "_kg.db"))
        if not _kg_exists:
            self._create_db()

        self._conn = self._connect()
        self._cursor = self._conn.cursor()

    def add_relationships(self, relationships: list[RelationshipModel]):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO relationships (head, head_type, relation, tail, tail_type, evidence, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                sanitize_term_for_search(relationship.head),
                relationship.head_type,
                relationship.relation,
                sanitize_term_for_search(relationship.tail),
                relationship.tail_type,
                json.dumps(relationship.evidence),
                relationship.source
            ) for relationship in relationships
        ])
        conn.commit()
        cursor.close()
        conn.close()

    def get_relationships(self, a_term: str, b_term: str, censor_year_lower: int = None, censor_year_upper: int = None, idx: Index = None) -> list[dict]:
        # TODO: handle logical operators
        
        a_term = sanitize_term_for_search(a_term)
        b_term = sanitize_term_for_search(b_term)

        cache_key = (a_term, b_term)
        results = self.cache.get(cache_key, None)
        if results is not None:
            return results

        results = []
        self._cursor.execute("""
            SELECT head, head_type, relation, tail, tail_type, evidence, source
            FROM relationships
            WHERE (head = ? AND tail = ?) OR (head = ? AND tail = ?)
        """, (a_term, b_term, b_term, a_term))

        rows = self._cursor.fetchall()
        for row in rows:
            head = row[0]
            head_type = row[1]
            rel = row[2]
            tail = row[3]
            tail_type = row[4]
            evidence = row[5]
            source = row[6]
            pmids = json.loads(evidence)[:100] if evidence else []

            result = {
                'a_term': head, 
                'a_type': head_type, 
                'b_term': tail, 
                'b_type': tail_type, 
                'relationship': rel, 
                'pmids': pmids, 
                'source': source
            }
            results.append(result)

        # date-censor the PMIDs
        if censor_year_lower or censor_year_upper:
            if idx is None:
                raise ValueError("Index must be provided for date-censoring relationships.")
            filtered_results = []
            for result in results:
                filtered_pmids = idx.date_censor_pmids(set(result['pmids']), censor_year_lower, censor_year_upper)
                if filtered_pmids:
                    result['pmids'] = list(filtered_pmids)
                    filtered_results.append(result)
            results = filtered_results

        self.cache[cache_key] = results
        return results

    def close(self):
        self._cursor.close()
        self._conn.close()

    def _connect(self):
        conn =  sqlite3.connect(os.path.join(self.data_dir, "_kg.db"))

        # use WAL and full synchronous for better corruption resistance
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=FULL;')
        
        return conn
    
    def _create_db(self):
        os.makedirs(self.data_dir, exist_ok=True)
        conn = self._connect()

        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS relationships (
                head TEXT NOT NULL,
                head_type TEXT NOT NULL,
                relation TEXT NOT NULL,
                tail TEXT NOT NULL,
                tail_type TEXT NOT NULL,
                evidence TEXT NOT NULL,
                source TEXT NOT NULL
            )
        """)
        conn.commit()

        # create indexes on head, tail
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_head ON relationships(head)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tail ON relationships(tail)")
        conn.commit()

        cursor.close()
        conn.close()