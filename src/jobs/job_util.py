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
def report_expansions(idx, terms: list[str], expand_synonyms: bool) -> None:
    """Record what each distinct term was searched as, once per term (not per result row), in
    the job's info as 'expansions': {term: [[subterm, [variants]], ...]}. Only when the job
    expands synonyms."""
    job = get_current_job()
    if not job or not expand_synonyms:
        return
    job.meta['expansions'] = {term: [[subterm, variants] for subterm, variants in idx.expansion(term)]
                              for term in dict.fromkeys(terms)}
    job.save_meta()
