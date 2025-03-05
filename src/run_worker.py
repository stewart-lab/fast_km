import multiprocessing
import time
import argparse
from workers.km_worker import start_worker
import workers.loaded_index as li
import indexing.km_util as km_util

parser = argparse.ArgumentParser()
parser.add_argument('-w', '--workers', default=1)
parser.add_argument('--high_priority', default=0, required=False)
parser.add_argument('--medium_priority', default=0, required=False)
parser.add_argument('--low_priority', default=0, required=False)
parser.add_argument('--neo4j_address', default='neo4j:7687', required=False)
parser.add_argument('--pubmed_api_key', default='', required=False)
parser.add_argument('--openai_api_key', default='', required=False)
parser.add_argument('--htcondor_token', default='', required=False)
args = parser.parse_args()

def start_workers(do_multiprocessing = True):
    n_workers = int(args.workers)
    high_priority = int(args.high_priority)
    medium_priority = int(args.medium_priority)
    low_priority = int(args.low_priority)
    km_util.neo4j_host = [x.strip() for x in args.neo4j_address.split(',')]
    km_util.pubmed_api_key = args.pubmed_api_key
    km_util.openai_api_key = args.openai_api_key
    km_util.htcondor_token = args.htcondor_token

    if do_multiprocessing:
        worker_processes = []
        for i in range(0, n_workers):
            queue_names = get_worker_queue_names(i, high_priority, medium_priority, low_priority)
            p = multiprocessing.Process(target=start_worker, args=(queue_names,))
            worker_processes.append(p)
            p.start()

        while True:
            # if a worker process is dead, restart it
            time.sleep(5)
            for i, worker in enumerate(worker_processes):
                if not worker or not worker.is_alive(): 
                    queue_names = get_worker_queue_names(i, high_priority, medium_priority, low_priority)
                    p = multiprocessing.Process(target=start_worker, args=(queue_names,))
                    worker_processes[i] = p
                    p.start()
            
    else:
        queue_names = get_worker_queue_names(i, high_priority, medium_priority, low_priority)
        start_worker(queue_names)

def get_worker_queue_names(i, high, med, low):
    if i < high:
        return [km_util.JobPriority.HIGH.name]
    elif i < high + med:
        return [km_util.JobPriority.MEDIUM.name]
    elif i < high + med + low:
        return [km_util.JobPriority.LOW.name]
    else:
        return [x.name for x in km_util.JobPriority]

def main():
    print('INFO: workers waiting 10 sec for redis to set up...')
    time.sleep(10)
    li.pubmed_path = '/mnt/pubmed'

    start_workers()

def debug():
    from indexing.index import Index
    import workers.kinderminer as km
    from workers.work import gpt_score_hypothesis

    # li.pubmed_path = '/w5home/rmillikin/PubmedAbstracts'
    # li.the_index = Index(li.pubmed_path)
    # km_result = km.kinderminer_search('cdk9', 'brd4', li.the_index)
    # print(km_result)

    # SKiM example
    example_data = [
        { 
            'a_term': 'breast cancer', 
            'b_term': 'CDK4', 
            'c_term': 'ABEMACICLIB',
            'ab_pmid_intersection': ['26030518', '19874578', '32940689', '33260316', '30130984'],
            'bc_pmid_intersection': ['27030077', '27217383', '34657059', '34958115', '37382948'],
            'ac_pmid_intersection': ['28580882', '28968163', '31250942', '33029704', '32955138']
        },
    ]

    example_hypotheses = {
        'AB': 'There exists a relationship between disease {a_term} and gene {b_term}.',
        'BC': 'There exists a relationship between gene {b_term} and drug {c_term}.',
        'rel_AC': 'There exists a relationship between disease {a_term} and drug {c_term}.',

        'ABC': 'The drug {c_term} can be used to treat the disease {a_term} through the gene {b_term}.',
        'AC': 'The drug {c_term} can be used to treat the disease {a_term}.',
    }

    config = {
        'data': example_data,
        'SKIM_hypotheses': example_hypotheses,
        'model': 'o3-mini', # optional. o3-mini is default
    }

    # KM example
    example_data = [
        { 
            'a_term': 'breast cancer', 
            'b_term': 'ABEMACICLIB', 
            'ab_pmid_intersection': ['28580882', '28968163', '31250942', '33029704', '32955138']
        },
    ]

    km_hypothesis = 'The drug {b_term} can be used to treat the disease {a_term}.'

    config = {
        'data': example_data,
        'KM_hypothesis': km_hypothesis,
        'model': 'o3-mini', # optional. o3-mini is default
    }

    
    

    results = gpt_score_hypothesis(config)
    print(str(results))





if __name__ == '__main__':
    main()
    # debug()