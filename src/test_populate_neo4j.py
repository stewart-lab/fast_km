from knowledge_graph.knowledge_graph import KnowledgeGraph
import workers.loaded_index as li
def main():
    #li.pubmed_path = 'C:\\Users\\rjmil\\Desktop\\kg_data'
    kg = KnowledgeGraph('localhost:7687')
    kg.populate('/Users/bmoore/Desktop/knowledge_graph/aggregated_top100.tsv')
    # kg.write_node_id_index()
    # print(kg.graph_name)
if __name__ == '__main__':
    main()