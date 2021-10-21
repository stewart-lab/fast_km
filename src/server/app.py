from flask import Flask, request
from flask import jsonify
import rq_dashboard
from redis import Redis
from rq import Queue
from flask_restful import Api
from workers.work import km_work
import logging

_r = Redis(host='redis', port=6379)
_q = Queue(connection=_r)
_app = Flask(__name__)
_api = Api(_app)

def start_server():
    # set up redis-queue dashboard
    _set_up_rq_dashboard()

    # disable non-error log messages
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    # start the server
    _app.run(host="0.0.0.0")

@_app.route('/km/api/jobs/', methods=['POST'])
def _post_km_job():
    if request.content_type != 'application/json':
        return 'Content type must be application/json', 400

    json_data = request.get_json(request.data)
    job = _q.enqueue(km_work, json_data)

    response = jsonify([{'id' : job.id}])
    response.status_code = 202
    return response

@_app.route('/km/api/jobs/', methods=['GET'])
def _get_km_job():
    id = request.args['id']
    job = _q.fetch_job(id)
    job_data = [{'status' : job.get_status()}]

    if job.result is not None:
        job_data.append({'result' : job.result})
        status_code = 200
    else:
        status_code = 202

    response = jsonify(job_data)
    response.status_code = status_code
    return response

def _set_up_rq_dashboard():
    _app.config.from_object(rq_dashboard.default_settings)
    _app.register_blueprint(rq_dashboard.blueprint, url_prefix="/rq")
    _app.config['RQ_DASHBOARD_REDIS_URL'] = 'redis://redis:6379'