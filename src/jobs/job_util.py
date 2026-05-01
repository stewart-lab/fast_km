from rq import get_current_job

def report_progress(progress: float) -> None:
    job = get_current_job()
    if not job:
        return
    progress = round(progress, 4)
    job.meta['progress'] = progress
    job.save_meta()

def report_log(file: str, message: str) -> None:
    job = get_current_job()
    if not job:
        return
    if 'logs' not in job.meta:
        job.meta['logs'] = dict()
    job.meta['logs'][file] = message
    job.save_meta()