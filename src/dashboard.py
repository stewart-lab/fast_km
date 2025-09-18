import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import streamlit as st
from rq import Worker, Queue
from rq.job import Job, JobStatus
from rq.registry import (
    StartedJobRegistry,
    FinishedJobRegistry, 
    FailedJobRegistry,
    DeferredJobRegistry,
    ScheduledJobRegistry,
    CanceledJobRegistry
)
import global_vars as gvars

args = sys.argv[1:]
redis_conn_info = args[0]
gvars.redis_host = redis_conn_info.split(':')[0]
gvars.redis_port = int(redis_conn_info.split(':')[1])
from redis_conn import redis_conn

## ---- Utility functions ----
def list_jobs() -> list[Job]:
    """Lists all jobs in all queues and registries"""
    jobs = []
    job_ids = set()
    queues = Queue.all(connection=redis_conn)
    for queue in queues:
        registries = [
            queue,
            FinishedJobRegistry(queue=queue),
            FailedJobRegistry(queue=queue),
            StartedJobRegistry(queue=queue),
            DeferredJobRegistry(queue=queue),
            ScheduledJobRegistry(queue=queue),
            CanceledJobRegistry(queue=queue),
        ]

        for registry in registries:
            for job_id in registry.get_job_ids():
                job_ids.add(job_id)
            
    for job_id in job_ids:
        try:
            job = Job.fetch(job_id, connection=redis_conn)
            if job is None:
                continue
            jobs.append(job)
        except Exception as e:
            print(f"Could not fetch job {job_id}: {e}")
    return jobs

def convert_timestamp_to_local(ts: float) -> str:
    """Convert a UTC timestamp to local time string"""

    # # get server timezone
    # server_tzinfo = datetime.now().astimezone().tzinfo
    # server_tzkey = getattr(server_tzinfo, "key", None)

    # # validate that server_tzname is a valid timezone
    # if server_tzkey:
    #     try:
    #         zone_info = ZoneInfo(server_tzkey)
    #         server_tzname = server_tzkey
    #     except Exception:
    #         server_tzname = "UTC"
    # else:
    #     server_tzname = "UTC"

    # convert server time to the timezone specified in global_vars
    # server_time = created_at.astimezone()
    # local_time = server_time.astimezone(ZoneInfo(gvars.timezone))

    dt_utc = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
    dt_local = dt_utc.astimezone(ZoneInfo(gvars.timezone))
    return dt_local.strftime(f"%Y-%m-%d %-I:%M:%S %p ({gvars.timezone} time)")

## ---- Streamlit app ----
st.set_page_config(
    page_title="Fast-KM Dashboard",
    layout="wide",
    page_icon="📚"
)

st.title("Fast-KM Dashboard")
now = datetime.now().strftime("%Y-%m-%d %H:%M")
now_local = convert_timestamp_to_local(datetime.now().timestamp())
st.markdown(f"Current time: {now_local}")

# show link to the API docs
# this won't necessarily be correct if Docker or port forwarding is used
st.markdown(f"API docs are available at [http://localhost:8000/docs](http://localhost:8000/docs)")

# list workers
st.subheader("Workers")
queue_names = ['HIGH', 'MEDIUM', 'LOW', 'INDEXING']
workers = Worker.all(connection=redis_conn)
workers.sort(key=lambda w: queue_names.index(w.queues[0].name) if w.queues else "")
for worker in workers:
    try:
        current_job = worker.get_current_job()
    except Exception:
        current_job = None
    current_job_id = current_job.id if current_job else "None"
    if worker.state in ['idle']:
        icon = "💤"
    elif worker.state in ['busy', 'started']:
        icon = "⏩"
    elif worker.state in ['suspended']:
        icon = "⏸️"
    else:
        icon = "❓"
    st.write(f"- **Worker:** {worker.name}, **State:** {icon} {worker.state}, **Current Job:** {current_job_id}, **Queues:** {str.join(',', [q.name for q in worker.queues])}")

if not workers:
    st.info("No workers found.")

# list jobs
st.subheader("Jobs")
jobs = list_jobs()
jobs.sort(key=lambda job: job.created_at or datetime.min, reverse=True)
for job in jobs:
    id = job.id
    status = job.get_status()
    created_at = job.created_at if job.created_at else None
    progress = job.meta.get('progress', None) if job.meta else None
    failure_reason = job.exc_info if job.exc_info else "No error message reported."

    try:
        max_len = 80
        description = job.description.split('.')[-1]
        description = description if len(description) < max_len else description[:max_len - 3] + "..."
    except Exception:
        description = "exception"

    test_cont = st.container()
    test_cont.empty()

    with st.container(border=True, key=f"container_{id}"):
        col1, col2, col3 = st.columns([1, 0.05, 0.035])

        # Job header with job name
        with col1:
            st.markdown(f"Job ID: _{id}_")
        
        with col2:
            # popover to show error message for failed jobs
            if status == JobStatus.FAILED:
                with st.popover("⚠️", help="View error"):
                    st.caption("Error message:")
                    st.code(failure_reason, language="python")
            else:
                # if you don't do this there is a weird ghosting effect as new jobs are added.
                # I think it always needs to have something to draw to refresh this area properly, not sure.
                st.write("")
        
        with col3:
            cancel_icon = "🚫"
            delete_icon = "✖️"

            if status in [JobStatus.QUEUED, JobStatus.STARTED, JobStatus.DEFERRED, JobStatus.SCHEDULED]:
                # button to cancel in-progress jobs
                if st.button(cancel_icon, key=f"cancel_{id}", help="Cancel job"):
                    job.cancel()
                    st.rerun()
            else:
                # button to delete finished/failed/canceled jobs
                if st.button(delete_icon, key=f"delete_{id}", help="Delete job"):
                    job.cleanup(ttl=0)
                    st.rerun()

        # Status badge
        if status == JobStatus.QUEUED:
            status_txt = ("🔵 Queued")
        elif status == JobStatus.STARTED:
            status_txt = ("🔵 Started")
        elif status == JobStatus.DEFERRED:
            status_txt = ("🔵 Deferred")
        elif status == JobStatus.SCHEDULED:
            status_txt = ("🔵 Scheduled")
        elif status == JobStatus.FINISHED:
            status_txt = ("🟢 Finished")
        elif status == JobStatus.FAILED:
            status_txt = ("🔴 Failed")
        elif status == JobStatus.CANCELED:
            status_txt = ("🔴 Canceled")
        elif status == JobStatus.STOPPED:
            status_txt = ("🔴 Stopped")
        else:
            status_txt = (f"🟠 {status}")

        # Progress bar
        if status == JobStatus.STARTED and progress is not None:
            st.progress(progress / 1.0, text=status_txt)
        elif status == JobStatus.FINISHED:
            st.progress(1.0, text=status_txt)
        else:
            st.progress(0.0, text=status_txt)

        col3, col4 = st.columns([1, 1])
        with col3:
            st.caption(f"Function: {description}")

        with col4:
            # Format creation date/time
            if created_at is None:
                label = "time unknown"
            else:
                date_str = convert_timestamp_to_local(created_at.timestamp())
                label = date_str

            st.caption(f"<div style='text-align: right;'>Submitted: {label}</div>", unsafe_allow_html=True)


if not jobs:
    st.info("No jobs found.")

# https://discuss.streamlit.io/t/looking-for-a-proper-way-of-resolving-ghost-shadow-mirage-left-by-widget-after-session-state-change-my-current-workaround-is-hacky/86638
display = st.empty()
for i in range(0, 100):
      st.markdown(" ")

# refresh the page every 1 second(s)
time.sleep(5)
st.rerun()