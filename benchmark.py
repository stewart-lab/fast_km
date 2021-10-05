import os
import datetime
import project.src.index_abstracts as indexer
import project.src.kinderminer as km
import project.src.km_util as util
from project.src.index import Index

def run_benchmark(the_index: Index, path_to_gene_names: str) -> None:
    """"""
    print('Starting benchmark...')
    n_grams = util.read_all_lines(path_to_gene_names)

    the_datetime = datetime.datetime.now()
    datetime_string = the_datetime.strftime("%m-%d-%Y-%H-%M-%S")
    filepath = os.path.join(os.path.dirname(path_to_gene_names), 'benchmark_' + datetime_string + '.tsv')

    n_queries = len(n_grams)
    a_term = "cancer"

    results = []
    for i in range(0, n_queries):
        b_term = n_grams[i]
        query_result = km.kinderminer_search(a_term, b_term, the_index)
        results.append(query_result)

    str_results = []
    str_results.append('a term\tb term\ta term num abstracts\tb term num abstracts\tp-value\tsort ratio\ttime (s)')
    for item in results:
        str_results.append(str(item[0]) + '\t' + str(item[1]) + '\t' + str(item[2]) + '\t' + str(item[3]) + '\t' + str(item[4]) + '\t' + str(item[5]) + '\t' + str(item[6]))

    # write benchmark results
    util.write_all_lines(filepath, str_results)
    print('Benchmark complete')

def main():
    """"""
    print('Input local abstracts directory: ')
    abstracts_dir = input()

    # load or create/write index
    the_index = indexer.index_abstracts(abstracts_dir)

    path_to_genes = os.path.join(indexer.get_index_dir(abstracts_dir), 'genes.txt')
    run_benchmark(the_index, path_to_genes)
    
# run the main method
if __name__ == '__main__':
    main()