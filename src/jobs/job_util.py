from rq import get_current_job

def report_progress(progress: float) -> None:
    job = get_current_job()
    if not job:
        return
    progress = round(progress, 4)
    job.meta['progress'] = progress
    job.save_meta()