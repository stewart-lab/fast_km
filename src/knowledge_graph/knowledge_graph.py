from py2neo import Graph, Node, Relationship, NodeMatcher
from py2neo.bulk import create_nodes, create_relationships, merge_relationships
from itertools import islice
import indexing.km_util as util

uri="bolt://neo4j:7687"
user = "neo4j"
password = "mypass"

rel_pvalue_cutoff = 1e-5
min_pmids_for_rel = 3

class KnowledgeGraph:
    def __init__(self):
        self.query_cache = dict()

        try:
            self.graph = Graph(uri, auth=(user, password))
        except:
            self.graph = None
            print('Could not find a neo4j knowledge graph database')

    def query(self, a_term: str, b_term: str):
        if not self.graph:
            return [{'a_term': a_term, 'a_type': '', 'relationship': 'neo4j connection error', 'b_term': b_term, 'b_type': '', 'pmids': []}]

        a_term_stripped = _sanitize_txt(a_term)
        b_term_stripped = _sanitize_txt(b_term)

        if (a_term_stripped, b_term_stripped) in self.query_cache:
            return self.query_cache[(a_term_stripped, b_term_stripped)]

        # get nodes from the neo4j database
        a_matches = self.graph.nodes.match(name=a_term_stripped).all()
        b_matches = self.graph.nodes.match(name=b_term_stripped).all()

        # get relationship(s) between a and b nodes
        relation_matches = []

        for a_node in a_matches:
            for b_node in b_matches:
                ab_rels = self.graph.match(nodes=(a_node, b_node)).all()
                relation_matches.extend(ab_rels)

                ba_rels = self.graph.match(nodes=(b_node, a_node)).all()
                relation_matches.extend(ba_rels)

        result = []

        for relation in relation_matches:
            # TODO: this is pretty hacky.
            # need to find a better way to retrieve node/relation types as strings.
            a_type = str(relation.nodes[0].labels).strip(':')
            b_type = str(relation.nodes[1].labels).strip(':')
            relationship = str(type(relation)).replace("'", "").replace(">", "").split('.')[2]

            relation_json = {'a_term': a_term, 'a_type': a_type, 'relationship': relationship, 'b_term': b_term, 'b_type': b_type, 'pmids':relation['pmids']}
            result.append(relation_json)

        if not result:
            result.append({'a_term': a_term, 'a_type': '', 'relationship': '', 'b_term': b_term, 'b_type': '', 'pmids': []})

        self.query_cache[(a_term_stripped, b_term_stripped)] = result
        return result

    def populate(self, path_to_tsv_file: str):
        self.graph.delete_all()

        node_types = ['CHEMICAL', 'CONDITION', 'DRUG', 'GGP', 'BIO_PROCESS']
        for node_type in node_types:
            try:
                self.graph.run("CREATE INDEX ON :" + node_type + "(name)")
            except:
                pass

        # add nodes
        nodes = {}

        with open(path_to_tsv_file, 'r') as f:
            for i, line in enumerate(f):
                if i == 0:
                    continue

                spl = line.strip().split('\t')

                node1_name = _sanitize_txt(spl[0])
                node1_type = spl[1]
                rel_txt = spl[2]
                node2_name = _sanitize_txt(spl[3])
                node2_type = spl[4]

                pmids = spl[len(spl) - 1].strip('}').strip('{')
                pmids = [int(x.strip()) for x in pmids.split(',')]

                if len(pmids) < min_pmids_for_rel:
                    continue

                if node1_type not in nodes:
                    nodes[node1_type] = set()
                if node2_type not in nodes:
                    nodes[node2_type] = set()

                nodes[node1_type].add(node1_name)
                nodes[node2_type].add(node2_name)

        for node_type, nodes_list in nodes.items():
            create_nodes(self.graph.auto(), [[x] for x in nodes_list], labels={node_type}, keys=["name"])
        nodes.clear()

        # add relations
        rels = {}
        with open(path_to_tsv_file, 'r') as f:
            for n_rel, line in enumerate(f):
                if n_rel == 0:
                    continue

                spl = line.strip().split('\t')

                node1_name = _sanitize_txt(spl[0])
                node1_type = spl[1]
                rel_txt = spl[2]
                node2_name = _sanitize_txt(spl[3])
                node2_type = spl[4]

                pmids = spl[len(spl) - 1].strip('}').strip('{')
                pmids = [int(x.strip()) for x in pmids.split(',')]

                if len(pmids) < min_pmids_for_rel:
                    continue

                category_txt = node1_type + ',' + rel_txt + ',' + node2_type

                if category_txt not in rels:
                    rels[category_txt] = []
                
                rels[category_txt].append(((node1_name), {"pmids": pmids}, (node2_name)))
                
                if (n_rel + 1) % 20000 == 0:
                    self._post_rels(rels)
                    rels.clear()
                
        self._post_rels(rels)
        rels.clear()

    def _post_rels(self, rels: dict):
        for rel, rel_nodes in rels.items():
            n1_type = rel.split(',')[0]
            r_type = rel.split(',')[1]
            n2_type = rel.split(',')[2]

            batch_size = 5000

            for batch in _group_elements(rel_nodes, batch_size):
                merge_relationships(self.graph.auto(), batch, r_type, start_node_key=(n1_type, "name"), end_node_key=(n2_type, "name"))

def _sanitize_txt(term: str):
    return str.join(' ', util.get_tokens(term.lower().strip()))

# batches "lst" into "chunk_size" sized elements
def _group_elements(lst, chunk_size):
    lst = iter(lst)
    return iter(lambda: tuple(islice(lst, chunk_size)), ())