from pydantic import BaseModel
from rq import Queue
from rq.job import Job, JobStatus
from rq.registry import StartedJobRegistry
from src.redis_conn import redis_conn
from src.jobs.index_corpus.job import run_indexing_job

_low_priority_queue = Queue('LOW', redis_conn)
_medium_priority_queue = Queue('MEDIUM', redis_conn)
_high_priority_queue = Queue('HIGH', redis_conn)
_index_queue = Queue('INDEXING', redis_conn)

HOUR = 60 * 60 # seconds in an hour

def queue_job(func, priority: str, params: BaseModel) -> dict:
    """Queues a job and returns the job ID"""
    if priority == _low_priority_queue.name:
        queue = _low_priority_queue
    elif priority == _medium_priority_queue.name:
        queue = _medium_priority_queue
    elif priority == _high_priority_queue.name:
        queue = _high_priority_queue
    else:
        print(f"WARNING: Received request with invalid job priority: '{priority}'")
        queue = _medium_priority_queue
    
    # if an indexing job is queued or running, this job should wait for that to finish first
    dependencies = _get_queued_or_running_index_job()
    job_id = params.id if hasattr(params, 'id') else None
    
    job = queue.enqueue(
        func, 
        params,
        job_timeout=12 * HOUR,
        failure_ttl=7 * 24 * HOUR,
        depends_on=dependencies if dependencies else None,
        job_id=job_id
    )
    
    return {'id': job.id, 'status': job.get_status().value}

def queue_indexing_job(params: BaseModel) -> dict:
    """
    Queues an indexing job and returns the job ID. The indexing job is a 
    special job type that cannot be run while any other jobs are running.
    """
    # only one indexing job should be queued or running at a time
    if _get_queued_or_running_index_job():
        return None
    
    # if any other job is queued or running, those should finish before the indexing job starts
    dependencies = _get_queued_or_running_non_indexing_jobs()
    job_id = params.id if hasattr(params, 'id') else None

    job = _index_queue.enqueue(
        run_indexing_job, 
        params, 
        job_timeout=72 * HOUR,
        failure_ttl=7 * 24 * HOUR,
        depends_on=dependencies if dependencies else None,
        job_id=job_id
    )
    return {'id': job.id, 'status': job.get_status().value}

def get_job(job_id: str) -> dict | None:
    """Gets information about a job"""
    job = Job.fetch(job_id, connection=redis_conn)
    if job is None:
        return None

    job_info = dict()
    job_meta = job.get_meta()
    job_info['id'] = job.id
    job_info['status'] = job.get_status().value
    job_info['result'] = job.result

    if job.exc_info:
        # if the job failed, include the error message.
        # this is intended for developers running the frontend, not end users.
        job_info['error'] = job.exc_info

        # some exceptions may be safe to show to the user and some won't be.
        # we won't know which is which. so we check to see if the error
        # message has a special marker that indicates it's safe to show.
        # if it does, we show that message. else we show a generic error message.
        spl = job.exc_info.split('-BEGIN USER-FACING ERROR-')
        if len(spl) == 2:
            job_info['user_facing_error'] = spl[1].split('-END USER-FACING ERROR-')[0].strip()
        else:
            job_info['user_facing_error'] = 'An error occurred during execution of this job.'

    if 'progress' in job_meta:
        job_info['progress'] = job_meta['progress']

    return job_info

def cancel_job(job_id: str) -> dict | None:
    job = Job.fetch(job_id, connection=redis_conn)
    if job is None:
        return None
    job.cancel()
    return {"id": job.id, "status": JobStatus.CANCELED.value}

def _get_queued_or_running_index_job() -> str:
    """Gets the ID of the indexing job if one is queued or running"""
    jobs_in_queue = _index_queue.jobs
    jobs_running = StartedJobRegistry(queue=_index_queue).get_job_ids()
    if len(jobs_in_queue) > 0:
        return jobs_in_queue[0].id
    if len(jobs_running) > 0:
        return jobs_running[0]
    return None

def _get_queued_or_running_non_indexing_jobs() -> list[str]:
    """Gets the IDs of all queued or running non-index jobs"""
    job_ids = []
    for queue in [_low_priority_queue, _medium_priority_queue, _high_priority_queue]:
        jobs_in_queue = queue.jobs
        jobs_running = StartedJobRegistry(queue=queue).get_job_ids()
        for job in jobs_in_queue:
            job_ids.append(job.id)
        for job_id in jobs_running:
            job_ids.append(job_id)
    return job_ids