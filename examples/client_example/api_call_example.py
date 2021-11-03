import requests
import time

# create the KM query details
the_json = [
    { "a_term": "cancer", "b_term": "tumor" }, 
    { "a_term": "cancer", "b_term": "" }
]

#with open('/path/to/my/gene/names/genes.txt', 'r') as file:
#    genes = file.readlines()

#the_json.clear()
#for gene in genes:
#    the_json.append({ "a_term": "cancer", "b_term": gene.strip() })

api_url = 'http://localhost:5000/km/api/jobs'

# queue a new KM job for the server to perform
response = requests.post(api_url, json=the_json)

# get the job's ID
json_post_response = response.json()
job_id = json_post_response[0]['id']

# get the status of the job
get_response = requests.get(api_url + "?id=" + job_id)
json_get_response = get_response.json()
job_status = json_get_response[0]['status']
print('job status is: ' + job_status)

# wait for job to complete, sleep
approx_total_time = 0
while job_status == 'queued' or job_status == 'started':
    print('sleeping for 10 sec...')
    time.sleep(10)

    # get the status of the job
    get_response = requests.get(api_url + "?id=" + job_id)
    json_get_response = get_response.json()
    job_status = json_get_response[0]['status']
    print('job status is: ' + job_status)
    approx_total_time += 10

# if the job's status is 'finished', print out the results
if job_status == 'finished':
    print(json_get_response[1]['result'])
    print("total query time: " + str(approx_total_time) + " sec")