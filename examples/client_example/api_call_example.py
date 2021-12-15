import requests
import time

# create the KM query details
the_json = [
    { "a_term": "cancer", "b_term": "tumor" }, 
    { "a_term": "cancer", "b_term": "malignant" }
]

#with open('/path/to/my/gene/names/genes.txt', 'r') as file:
#    genes = file.readlines()

#the_json.clear()
#for gene in genes:
#    the_json.append({ "a_term": "cancer", "b_term": gene.strip() })

api_url = 'http://localhost:5001/km/api/jobs'

# queue a new KM job for the server to perform
response = requests.post(api_url, json=the_json).json()

# get the job's ID
job_id = response['id']

# get the status of the job
get_response = requests.get(api_url + "?id=" + job_id).json()
job_status = get_response['status']
print('job status is: ' + job_status)

# wait for job to complete, sleep
approx_total_time = 0
while job_status == 'queued' or job_status == 'started':
    print('sleeping for 10 sec...')
    time.sleep(10)

    # get the status of the job
    get_response = requests.get(api_url + "?id=" + job_id).json()
    job_status = get_response['status']
    print('job status is: ' + job_status)
    approx_total_time += 10

# if the job's status is 'finished', print out the results
if job_status == 'finished':
    print(get_response['result'])
    print("total query time: " + str(approx_total_time) + " sec")