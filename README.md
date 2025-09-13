# KinderMiner Web API Server
Welcome to the codebase for the KinderMiner web API server. If you are interested 
in running a KinderMiner query, please visit https://skim.morgridge.org/ .

## What is KinderMiner?
KinderMiner is a text search algorithm. https://pmc.ncbi.nlm.nih.gov/articles/PMC8756297/

Serial KinderMiner is KinderMiner x2. https://pubmed.ncbi.nlm.nih.gov/37915001/

SKiM-GPT adds LLM stuff. https://www.biorxiv.org/content/10.1101/2025.07.28.664797v1.full

## Starting the server
You can start via shell script:

```bash
# make sure redis is running
docker run --name some-redis -p 6379:6379 -d redis

# start the API server
python3 app.py
```

or with docker-compose:
```bash
docker compose up --build
```

## Submitting queries
You will need to build the database first. 

Then, run:

```python
import requests
import time

# define the job parameters
km_params = {'a_terms': ['breast cancer'], 'b_terms': ['ABEMACICLIB']}

# submit the job
job = requests.post('http://localhost:8000/api/kinderminer', json=km_params).json()
print(f'Submitted job: {job['id']}')

# wait for the job to complete
for _ in range(30):
    time.sleep(1)
    job = requests.get(f'http://localhost:8000/api/kinderminer?id={job['id']}').json()

    if job['status'] in ['finished', 'failed']:
        print(f'Job ended with result: {job}')
        break
    else:
        print(f'Job status: {job['status']}')
```

## More stuff
View the dashboard

View the API docs