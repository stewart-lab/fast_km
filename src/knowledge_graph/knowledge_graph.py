from py2neo import Graph, Node, Relationship, NodeMatcher
from py2neo.bulk import create_nodes, create_relationships
from itertools import islice
import indexing.km_util as util

uri="bolt://neo4j:7687"
user = "neo4j"
password = "mypass"

class KnowledgeGraph:
    def __init__(self):
        self.graph = Graph(uri, auth=(user, password))

    def query(self, a_term: str, b_term: str):
        # get a-term nodes
        a_term_stripped = a_term.lower().strip()
        a_term_stripped = str.join(' ', util.get_tokens(a_term_stripped))
        a_matches = self.graph.nodes.match(name=a_term_stripped).all()

        # get b-term nodes
        b_term_stripped = b_term.lower().strip()
        b_term_stripped = str.join(' ', util.get_tokens(b_term_stripped))
        b_matches = self.graph.nodes.match(name=b_term_stripped).all()

        # get relationship(s) between a and b nodes
        relation_matches = []

        for a_node in a_matches:
            for b_node in b_matches:
                ab_rels = self.graph.match(nodes=(a_node, b_node)).all()
                relation_matches.extend(ab_rels)

                ba_rels = self.graph.match(nodes=(b_node, a_node)).all()
                relation_matches.extend(ba_rels)

        return_text = []

        for relation in relation_matches:
            # TODO: this is pretty hacky.
            # need to find a better way to retrieve node/relation types as strings.
            a_type = str(relation.nodes[0].labels).strip(':')
            b_type = str(relation.nodes[1].labels).strip(':')
            relationship = str(type(relation)).replace("'", "").replace(">", "").split('.')[2]

            return_text.append(str.join('', [a_term, '[', a_type, '] ', relationship, ' ', b_term, '[', b_type, ']']))

        return str.join('; ', return_text)