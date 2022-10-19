from flask import Flask, request
from flask import jsonify
import rq_dashboard
from redis import Redis
from rq import Queue
from rq.job import Job
from rq.command import send_stop_job_command
from rq.exceptions import InvalidJobOperation
from flask_restful import Api
from workers.work import km_work, km_work_all_vs_all, update_index_work, clear_mongo_cache
import logging
from flask_bcrypt import Bcrypt

_r = Redis(host='redis', port=6379)
_q = Queue(connection=_r)
_app = Flask(__name__)
_api = Api(_app)
_bcrypt = Bcrypt(_app)
_pw_hash = ''

def start_server(pw_hash: str):
    global _pw_hash
    _pw_hash = pw_hash.replace('____', '$')

    # set up redis-queue dashboard
    _set_up_rq_dashboard()

    # disable non-error log messages
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    # start the server
    _app.run(host="0.0.0.0")

def _set_up_rq_dashboard():
    _app.config.from_object(rq_dashboard.default_settings)
    _app.register_blueprint(rq_dashboard.blueprint, url_prefix="/rq")
    _app.config['RQ_DASHBOARD_REDIS_URL'] = 'redis://redis:6379'

def _authenticate(request):
    if _pw_hash == 'none':
        return True

    if request.authorization and 'password' in request.authorization:
        candidate = request.authorization['password']
    else:
        return False

    return _bcrypt.check_password_hash(_pw_hash, candidate)

## ******** Generic Post/Get ********
def _post_generic(work, request, job_timeout = 43200):
    if request.content_type != 'application/json':
        return 'Content type must be application/json', 400

    # NOTE: the max amount of time a job is allowed to take is 12 hrs by default

    json_data = request.get_json(request.data)

    if not _authenticate(request):
        return 'Invalid password. do request.post(..., auth=(\'username\', \'password\'))', 401

    # If the job is posted with an id lets use it
    if 'id' in json_data:
        job = _q.enqueue(work, json_data, job_timeout=job_timeout, job_id=json_data['id'])
    else:
        job = _q.enqueue(work, json_data, job_timeout=job_timeout)

    job_data = dict()
    job_data['id'] = job.id
    job_data['status'] = job.get_status()

    response = jsonify(job_data)
    response.status_code = 202
    return response

def _get_generic(request):
    if not _authenticate(request):
        return 'Invalid password. do request.get(..., auth=(\'username\', \'password\'))', 401

    id = request.args['id']
    job_data = dict()
    job_data['id'] = id

    job = _q.fetch_job(id)

    if job:
        job_data['status'] = job.get_status()
        meta = job.get_meta()

        if 'progress' in meta:
            job_data['progress'] = meta['progress']

        if job.result is not None:
            job_data['result'] = job.result
            status_code = 200
        else:
            status_code = 202
    else:
        job_data['status'] = 'not_found'
        status_code = 404

    response = jsonify(job_data)
    response.status_code = status_code
    return response

## ******** KinderMiner Post/Get ********
@_app.route('/km/api/jobs/', methods=['POST'])
def _post_km_job():
    return _post_generic(km_work, request)

@_app.route('/km/api/jobs/', methods=['GET'])
def _get_km_job():
    return _get_generic(request)

## ******** SKiM Post/Get ********
@_app.route('/skim/api/jobs/', methods=['POST'])
def _post_skim_job():
    return _post_generic(km_work_all_vs_all, request)

@_app.route('/skim/api/jobs/', methods=['GET'])
def _get_skim_job():
    return _get_generic(request)

## ******** Update Index Post/Get ********
@_app.route('/update_index/api/jobs/', methods=['POST'])
def _post_update_index_job():
    return _post_generic(update_index_work, request, job_timeout=172800)

@_app.route('/update_index/api/jobs/', methods=['GET'])
def _get_update_index_job():
    return _get_generic(request)

## ******** Clear MongoDB Cache Post ********
@_app.route('/clear_cache/api/jobs/', methods=['POST'])
def _post_clear_cache_job():
    return _post_generic(clear_mongo_cache, request)

## ******** Cancel Job Post ********
@_app.route('/cancel_job/api/jobs/', methods=['POST'])
def _post_cancel_job():
    json_data = request.get_json(request.data)
    job_id = json_data['id']
    response = jsonify(dict())

    job = Job.fetch(job_id, connection=_r)

    if job:
        job.cancel()
        try:
            send_stop_job_command(connection=_r, job_id=job_id)

            # TODO: can't seem to get an error message to display in 
            # rq-dashboard. probably should come back to this at some point.
            
            # job.exc_info = 'Job was canceled by request'
            # job.save()
        except InvalidJobOperation:
            # probably tried to cancel a job that wasn't in progress. 
            # just ignore the error message.
            pass
        status_code = 200
    else:
        status_code = 404

    response.status_code = status_code
    return response