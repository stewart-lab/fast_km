import os
import json
import time
import requests

port = 8000
kg_url = 'http://localhost:' + str(port) + '/api/knowledge_graph'
kg_file = "./_data/aggregated_rels.tsv" # header: NAME, TAG, REL, NAME, TAG, PMID
SOURCE = "DATA SOURCE GOES HERE"       # set this to the source of the data, e.g. "SemMedDB 2023"

def populate_kg():
    rels = []
    with open(kg_file, 'r') as f:
        for line in f:
            spl = line.strip().split('\t')
            if 'name' in line.lower() and 'tag' in line.lower():
                continue  # skip header line

            head = spl[0]
            head_type = spl[1]
            relation = spl[2]
            tail = spl[3]
            tail_type = spl[4]
            evidence = spl[5].replace('"', '')  # ensure JSON compatibility
            evidence = json.loads(evidence) if evidence else []

            # assert typeof evidence is list[int]
            assert isinstance(evidence, list) and all(isinstance(i, int) for i in evidence), f"Evidence is not a list of integers: {evidence}"

            if len(evidence) < 2:
                continue

            rel = {
                "head": head,
                "head_type": head_type,
                "relation": relation,
                "tail": tail,
                "tail_type": tail_type,
                "evidence": evidence,
                "source": SOURCE
            }
            rels.append(rel)

            if len(rels) >= 10000:
                payload = {"relationships": rels}
                response = requests.post(kg_url, json=payload).json()
                print(f"Added {len(rels)} relationships: {response}")
                rels = []
                time.sleep(1)  # to avoid overwhelming the server
    
    if rels:
        payload = {"relationships": rels}
        response = requests.post(kg_url, json=payload).json()
        print(f"Added {len(rels)} relationships: {response}")

    # query some relationships
    query_payload = {"entity1": "Breast Cancer", "entity2": "BRCA1"}
    response = requests.get(kg_url, json=query_payload).json()
    print(f"Queried relationships between BRCA1 and Breast Cancer: {response}")


if __name__ == "__main__":
    populate_kg()