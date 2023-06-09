# fast_km
[![AppVeyor Badge](https://ci.appveyor.com/api/projects/status/github/stewart-lab/fast_km)](https://ci.appveyor.com/project/robertmillikin/fast-km)

# API
## /skim/api/jobs/
### POST:
Post a SKiM job to the job queue. Must be in json format. Returns the job ID.
Parameters:
A dictionaries with the keys:
- "a_terms": list of strings
- "b_terms": list of strings
- "c_terms": list of strings
- "top_n": integer
- "ab_fet_threshold": float
- "censor_year" (optional): integer
- "return_pmids" (optional): boolean

Example:
```py
import requests
post_json = { "a_terms": ["cancer"], "b_terms": ["tumor"], "c_terms": ["skin"], "top_n": 50, "ab_fet_threshold": 0.01 }
response = requests.post('http://localhost:5000/skim/api/jobs', json=post_json).json()
job_id = response['id']
```

Response:
```py
{ 
  'id': 'abcd', 
  'status': 'submitted' 
}
```

### GET:
Returns the job's status given its ID, and, if the job is finished, the results of the job.

A finished job contains in the 'results' key a list of dictionaries, each with the key:
- "a_term": string
- "b_term": string
- "c_term": string
- "ab_pvalue": float
- "ab_sort_ratio": float
- "ab_pred_score": float
- "bc_pvalue": float
- "bc_sort_ratio": float
- "bc_pred_score": float
- "a_count": integer
- "b_count": integer
- "c_count": integer
- "ab_count": integer
- "bc_count": integer
- "total_count": integer
- "ab_pmid_intersection": set of integers, cast as a string
- "bc_pmid_intersection": set of integers, cast as a string

Example: 
```py
import requests
get_response = requests.get("http://localhost:5000/skim/api/jobs?id=" + job_id).json()
job_status = get_response['status']
if job_status == 'finished':
  job_result = get_response['result']
```

Response:
```py
{ 
  'id': 'abcd', 
  'status': 'finished', 
  'result': [{ "a_term": "cancer", "b_term": "tumor", ... }] 
}
```
