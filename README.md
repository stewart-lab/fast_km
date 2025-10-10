# KinderMiner Web API Server

A web API server for running KinderMiner, Serial KinderMiner (SKiM), and hypothesis evaluation text mining queries against a biomedical literature database.

## What is KinderMiner?

KinderMiner is a literature-based discovery algorithm that identifies statistical associations between biomedical concepts (genes, diseases, drugs, etc.) by analyzing their co-occurrence patterns in PubMed abstracts.

- **KinderMiner** tests pairwise relationships between two sets of terms (A-terms and B-terms)
- **Serial KinderMiner (SKiM)** discovers indirect A→C relationships through intermediate terms (A→B→C)
- **SKiM-GPT** adds LLM-based hypothesis evaluation to assess the biological plausibility of relationships discovered by KM or SKiM

If you just want to run a query without starting your own server, please visit https://skim.morgridge.org/ .

### Publications
- [KinderMiner paper](https://pmc.ncbi.nlm.nih.gov/articles/PMC8756297/)
- [Serial KinderMiner paper](https://pubmed.ncbi.nlm.nih.gov/37915001/)
- [SKiM-GPT preprint](https://www.biorxiv.org/content/10.1101/2025.07.28.664797v1.full)

## Architecture Overview

The system consists of several components:

1. **Server** - FastAPI web server with endpoints for submitting jobs and getting their status, and for adding documents to the literature database
2. **Job queue** - Redis instance that stores queued jobs
3. **Workers** - Background processes that process jobs from the job queue
4. **Database** - SQLite database that stores literature to search (PubMed abstracts)
5. **Dashboard** - Streamlit web UI for monitoring jobs and workers
6. **HTCondor** - An HTCondor cluster is required if running hypothesis evaluation jobs

## Running the Server

### Option 1: Run with Docker Compose
If you don't want to download or modify the code, you can use a pre-built image:

```yaml
services:
  server:
    image: rmillikin/fast-km:latest
    ports:
      - "8000:8000"
      - "8501:8501"
    volumes:
      - ./_data:/app/_data
      - /tmp:/tmp
    environment:
      - REDIS=redis:6379
    depends_on:
      - redis
    networks:
      - fast_km-network

  redis:
    image: redis
    networks:
      - fast_km-network

networks:
  fast_km-network:
```

The server will start with:
- API server on port 8000
- Dashboard on port 8501

### Option 2: Build and run with Docker Compose

If you want to modify the code you can clone the repository and run:

```bash
docker compose up --build
```

### Option 3: Manual Setup

If you prefer to run components separately, which can be nice for debugging:

```bash
# Start Redis
docker run --name redis -p 6379:6379 -d redis

# Create virtual environment and install dependencies
python3 -m venv .venv
source ./.venv/bin/activate # if on Windows, run .venv\Scripts\activate
pip install -r requirements.txt

# Start the server
python3 app.py
```

### Configuration

Configuration can be provided via environment variables. Create an .env file (see env.example):

```bash
HIGH=1   # number of workers for high-priority jobs
MEDIUM=1 # number of workers for medium-priority jobs
LOW=1    # number of workers for low-priority jobs
REDIS=localhost:6379
API_PORT=8000
TIMEZONE=America/Chicago

# Optional: API keys for hypothesis evaluation jobs
PUBMED_API_KEY=your_key_here
OPENAI_API_KEY=your_key_here
HTCONDOR_TOKEN=your_token_here
DEEPSEEK_API_KEY=your_key_here
```

## Populating the Database

Before running queries, you need to populate the database with documents. 
You can add documents and index them for querying via API:

```python
# add a document
# https://pubmed.ncbi.nlm.nih.gov/1/
doc = { 'pmid': 1, 'pub_year': 1975, 'title': 'Formate assay in body fluids: application in methanol poisoning' }

# run an indexing job

# run a search
```

To populate the database, you can download and add PubMed abstracts with the provided script:

```bash
python3 populate_db.py
```

This script will:
1. Check which files have already been downloaded
2. Download files from PubMed's FTP server (baseline + updates)
3. Parse the downloaded files and add documents to the database via the API
4. Optionally add citation count data from iCite (if `_icite` folder exists)
5. Build the search index

**Citation data (optional but recommended):**
To include citation counts, download iCite database snapshots from:
https://nih.figshare.com/collections/iCite_Database_Snapshots_NIH_Open_Citation_Collection_/4586573

Download the .tar.gz and extract the `.json` files into an `_icite` folder before running `populate_db.py`.

**Note:** The initial indexing job can take many hours. Progress can be monitored via the dashboard.

## Running Jobs

### KinderMiner Query

Search for associations between disease terms and drug terms:

```python
import requests
import time

# Submit a KinderMiner job
km_params = {
    'a_terms': ['breast cancer', 'lung cancer'],
    'b_terms': ['ABEMACICLIB', 'OSIMERTINIB'],
    'return_pmids': True,
    'top_n_articles_most_recent': 10
}

response = requests.post('http://localhost:8000/api/kinderminer', json=km_params)
job = response.json()
print(f'Submitted job: {job["id"]}')

# Wait for job to finish
while True:
    time.sleep(2)
    response = requests.get(f'http://localhost:8000/api/kinderminer?id={job["id"]}')
    job = response.json()
    
    if job['status'] in ['finished', 'failed']:
        print(f'Job completed: {job}')
        break
    else:
        progress = job.get('progress', 0)
        print(f'Progress: {progress:.1%}')
```

### API Documentation

Autogenerated API documentation (server must be running):
```
http://localhost:8000/docs
```

## Monitoring

View real-time job status and worker activity (server must be running):
```
http://localhost:8501/
```

The dashboard shows:
- Active workers and their current jobs
- All queued, running, finished, and failed jobs
- Job progress for running jobs
- Buttons to cancel/delete jobs