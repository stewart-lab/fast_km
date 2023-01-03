from py2neo import Graph
from py2neo.bulk import create_nodes, merge_relationships
from itertools import islice
import indexing.km_util as util
import indexing.index as index
import workers.loaded_index as li

uri="bolt://" + util.neo4j_host + ":7687"
user = "neo4j"
password = "mypass"

rel_pvalue_cutoff = 1e-5
min_pmids_for_rel = 3
max_synonyms = 9999

class KnowledgeGraph:
    def __init__(self):
        self.query_cache = dict()
        self.node_ids = dict()

        try:
            self.graph = Graph(uri, auth=(user, password))
        except:
            self.graph = None
            print('Could not find a neo4j knowledge graph database')
            return

        try:
            kg_ids = util.get_knowledge_graph_node_id_index(li.pubmed_path)
            if kg_ids:
                self.load_node_id_index(kg_ids)
        except:
            self.node_ids = dict()
            print('Problem loading graph node IDs')

    def query(self, a_term: str, b_term: str, censor_year = None):
        if not self.graph:
            return [{'a_term': a_term, 'a_type': '', 'relationship': 'neo4j connection error', 'b_term': b_term, 'b_type': '', 'pmids': []}]

        if index.logical_and in a_term or index.logical_and in b_term:
            return [self._null_rel_response(a_term, b_term)]

        a_term_stripped = _sanitize_txt(a_term)[:max_synonyms]
        b_term_stripped = _sanitize_txt(b_term)[:max_synonyms]
        sanitized_ab_tuple = (str.join(index.logical_or, a_term_stripped), str.join(index.logical_or, b_term_stripped))

        if sanitized_ab_tuple in self.query_cache:
            return self.query_cache[sanitized_ab_tuple]

        # get nodes from the neo4j database
        a_matches = []
        b_matches = []
        if self.node_ids:
            for a_subterm in a_term_stripped:
                if a_subterm in self.node_ids:
                    _id = self.node_ids[a_subterm]
                    a_matches.extend(self.graph.nodes.get(_id))
            for b_subterm in b_term_stripped:
                if b_subterm in self.node_ids:
                    _id = self.node_ids[b_subterm]
                    b_matches.extend(self.graph.nodes.get(_id))
        else:
            # this is ~50x slower than looking up by node ID but it will still work
            # TODO: implement synonym searching? right now only searches first one
            a_matches = self.graph.nodes.match(name=a_term_stripped[0]).all()
            b_matches = self.graph.nodes.match(name=b_term_stripped[0]).all()

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
            node1_name = relation.nodes[0]['name']
            node1_type = str(relation.nodes[0].labels).strip(':')
            
            node2_name = relation.nodes[1]['name']
            node2_type = str(relation.nodes[1].labels).strip(':')
            
            relationship = str(type(relation)).replace("'", "").replace(">", "").split('.')[2]

            if censor_year:
                if censor_year not in li.the_index._date_censored_pmids:
                    if not li.the_index._publication_years:
                        li.the_index._init_pub_years()

                    censored_set = set()

                    for pmid, year in li.the_index._publication_years.items():
                        if year <= censor_year:
                            censored_set.add(pmid)
                    li.the_index._date_censored_pmids[censor_year] = censored_set
                else:
                    censored_set = li.the_index._date_censored_pmids[censor_year]

                pmids = list(set(relation['pmids']) & censored_set)

                if not pmids:
                    continue
            else:
                pmids = relation['pmids']

            relation_json = {'a_term': node1_name, 'a_type': node1_type, 'relationship': relationship, 'b_term': node2_name, 'b_type': node2_type, 'pmids': pmids[:100]}
            result.append(relation_json)

        if not result:
            result.append(self._null_rel_response(a_term, b_term))

        self.query_cache[sanitized_ab_tuple] = result
        return result

    def write_node_id_index(self, path: str):
        all_nodes = self.graph.nodes.match()
        with open(path, 'w') as f:
            for node in all_nodes:
                name = node['name']
                ident = node.identity
                f.write(name + '\t' + str(ident) + '\n')

    def load_node_id_index(self, path: str):
        with open(path, 'r') as f:
            for line in f:
                spl = line.split('\t')
                name = spl[0]
                id = int(spl[1])

                if name not in self.node_ids:
                    self.node_ids[name] = []

                self.node_ids[name].append(id)

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

    def _null_rel_response(self, a_term, b_term):
        {'a_term': a_term, 'a_type': '', 'relationship': '', 'b_term': b_term, 'b_type': '', 'pmids': []}

def _sanitize_txt(term: str):
    subterms = set()
    terms = term.split(index.logical_or)
    for term in terms:
        subterms.add(str.join(' ', util.get_tokens(term.lower().strip())))
    return list(subterms)

# batches "lst" into "chunk_size" sized elements
def _group_elements(lst, chunk_size):
    lst = iter(lst)
    return iter(lambda: tuple(islice(lst, chunk_size)), ())