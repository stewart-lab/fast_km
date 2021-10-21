from flask import Flask, request
from flask import jsonify
import rq_dashboard
from redis import Redis
from rq import Queue
from flask_restful import Api
from project.src.work import km_work

r = Redis(host='redis', port=6379)
q = Queue(connection=r)
app = Flask(__name__)
api = Api(app)

@app.route('/km/api/jobs/', methods=['POST'])
def post_km_job():
    if request.content_type != 'application/json':
        return 'Content type must be application/json', 400

    json_data = request.get_json(request.data)
    job = q.enqueue(km_work, json_data)

    response = jsonify([{'id' : job.id}])
    response.status_code = 202
    return response

@app.route('/km/api/jobs/', methods=['GET'])
def get_km_job():
    id = request.args['id']
    job = q.fetch_job(id)
    job_data = [{'status' : job.get_status()}]

    if job.result is not None:
        job_data.append({'result' : job.result})
        status_code = 200
    else:
        status_code = 202

    response = jsonify(job_data)
    response.status_code = status_code
    return response

def set_up_rq_dashboard():
    app.config.from_object(rq_dashboard.default_settings)
    app.register_blueprint(rq_dashboard.blueprint, url_prefix="/rq")
    app.config['RQ_DASHBOARD_REDIS_URL'] = 'redis://redis:6379'

def main():
    # set up redis-queue dashboard
    set_up_rq_dashboard()

    # start the server
    app.run(host="0.0.0.0")
    
if __name__ == '__main__':
    main()