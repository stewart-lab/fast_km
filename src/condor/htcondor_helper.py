import htcondor
import time
import os
from typing import List, Dict, Any
from condor.utils import setup_logger

# Get the centralized logger instance
logger = setup_logger()

class HTCondorHelper:
    def __init__(self, config: Dict[str, Any]):
        """Initialize HTCondor helper with configuration"""
        self.config = config
        self.token = config["token"]
        # Remove debug logging
        # htcondor.enable_debug()
        self._setup_connection()
        logger.info("HTCondor helper initialized")

    def _setup_connection(self):
        """Setup connection to HTCondor submit node"""
        try:
            # Disable SciTokens cache directory setting and transfer history
            os.environ['_CONDOR_SCITOKENS_DISABLE_CACHE'] = 'true'
            os.environ['_CONDOR_STATS_HISTORY_FILE'] = '/dev/null'
            
            self.collector = htcondor.Collector(self.config["collector_host"])
            test = htcondor.classad.quote(self.config["submit_host"])
            
            # Setup security manager with token
            with htcondor.SecMan() as sess:
                sess.setToken(htcondor.Token(self.token))
                schedd_ad = self.collector.query(
                    htcondor.AdTypes.Schedd,
                    constraint=f"Name=?={test}",
                    projection=["Name", "MyAddress", "DaemonCoreDutyCycle"]
                )[0]
                self.schedd = htcondor.Schedd(schedd_ad)
            logger.info("Successfully connected to HTCondor submit node")
        except Exception as e:
            logger.error(f"Failed to setup HTCondor connection: {e}")
            raise

    def submit_jobs(self, submit_config: dict) -> int:
        """Submit jobs to HTCondor using files.txt"""
        try:
            # create the Submit object
            submit_obj = htcondor.Submit(submit_config)

            # Submit the job
            with htcondor.SecMan() as sess:
                sess.setToken(htcondor.Token(self.token))
                result = self.schedd.submit(submit_obj, spool=True)
                cluster_id = result.cluster()

                # Spool the files
                # this transfers files from the current machine to the submit node
                self.schedd.spool(list(submit_obj.jobs(clusterid=cluster_id)))
                
            logger.info(f"Successfully submitted job to cluster {cluster_id}")
            return cluster_id

        except Exception as e:
            logger.error(f"Failed to submit jobs: {e}")
            raise

    def monitor_jobs(self, cluster_id: int, check_interval: int = 30) -> bool:
        total_wait = 0

        """Monitor job progress"""
        try:
            with htcondor.SecMan() as sess:
                sess.setToken(htcondor.Token(self.token))
                while True:
                    ads = self.schedd.query(
                        constraint=f"ClusterId == {cluster_id}",
                        projection=["ProcID", "JobStatus"]
                    )
                    
                    if not ads:
                        logger.warning(f"No jobs found for cluster {cluster_id}")
                        return False
                    
                    # Check if all jobs completed
                    if all(ad.get("JobStatus") == 4 for ad in ads):
                        logger.info(f"All jobs in cluster {cluster_id} completed successfully")
                        return True
                    
                    # Retrieve output files every minute
                    # logger.info(f"Attempting to retrieve intermediate output files for cluster {cluster_id}")
                    # try:
                    #     self.schedd.retrieve(f"ClusterId == {cluster_id}")
                    #     logger.info(f"Successfully retrieved intermediate output files for cluster {cluster_id}")
                    # except Exception as retrieve_err:
                    #     logger.warning(f"Warning: Failed to retrieve intermediate output files: {retrieve_err}")
                    
                    # Log status counts
                    status_counts = {}
                    for ad in ads:
                        status = ad.get("JobStatus", 0)
                        status_counts[status] = status_counts.get(status, 0) + 1
                    
                    status_desc = {
                        1: "Idle", 
                        2: "Running", 
                        3: "Removed",
                        4: "Completed", 
                        5: "Held", 
                        6: "Transferring Output",
                        7: "Suspended"
                    }
                    
                    status_msg = ", ".join(f"{status_desc.get(k, f'Unknown({k})')}: {v}" 
                                         for k, v in status_counts.items())
                    logger.info(f"Cluster {cluster_id} status: {status_msg}. Total wait time: {total_wait} seconds")
                    
                    # for some reason, jobs get 'held' when submitted instead of 'idle'. not sure why.
                    # they do seem to go to 'idle' shortly thereafter though.
                    # 'held' jobs basically also mean 'failed' jobs.
                    # so we will say that if a job is not 'idle' or 'running' after 2 minutes, it has failed.
                    # it's a bit hacky...
                    if total_wait >= 120:
                        job_is_idle = status_counts.get(1, 0) > 0
                        job_is_running = status_counts.get(2, 0) > 0
                        if not job_is_idle and not job_is_running:
                            raise Exception(f"Cluster {cluster_id} timed out (job was held too long)")

                    time.sleep(check_interval)
                    total_wait += check_interval

        except Exception as e:
            logger.error(f"Error monitoring jobs: {e}")
            raise

    def retrieve_output(self, cluster_id: int):
        """Retrieve output files from completed jobs"""
        try:
            with htcondor.SecMan() as sess:
                sess.setToken(htcondor.Token(self.token))
                self.schedd.retrieve(f"ClusterId == {cluster_id}")
            logger.info(f"Successfully retrieved output for cluster {cluster_id}")
        except Exception as e:
            logger.error(f"Failed to retrieve output: {e}")
            raise