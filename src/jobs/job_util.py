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
def report_expansions(idx, terms: list[str]) -> None:
    """Record what each distinct term was searched as, as the index searched it, once per term
    (not per result row), in the job's info as 'expansions': {term: [[subterm, [variants]], ...]}.
    Only terms that matched other names are listed, which keeps big jobs small. Only when the
    index expands synonyms."""
    job = get_current_job()
    if not job or not idx.expand_synonyms:
        return
    job.meta['expansions'] = {term: idx.searched_expansions[term] for term in dict.fromkeys(terms)
                              if term in idx.searched_expansions}
    job.save_meta()
