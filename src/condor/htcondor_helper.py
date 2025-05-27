import htcondor2 as htcondor
import time
import os
from typing import Any
from condor.utils import setup_logger

# Get the centralized logger instance
logger = setup_logger()

class HTCondorHelper:
    def __init__(self, config: 'dict[str, Any]'):
        """Initialize HTCondor helper with configuration"""
        self.config = config
        self.token = config["token"]
        self.token_file = config["token_file"]
        # Remove debug logging
        # htcondor.enable_debug()
        self._setup_connection()
        logger.info("HTCondor helper initialized")

    def _setup_connection(self):
        """Setup connection to HTCondor submit node"""
        try:
            if not os.path.exists(self.token_file):
                raise ValueError(f"Token file not found at {self.token_file}")
            
            abs_token_dir = os.path.abspath(os.path.dirname(self.token_file))
            htcondor.param["SEC_TOKEN_DIRECTORY"] = abs_token_dir
            htcondor.param["SEC_CLIENT_AUTHENTICATION_METHODS"] = "TOKEN"
            htcondor.param["SEC_DEFAULT_AUTHENTICATION_METHODS"] = "TOKEN"
            htcondor.param["SEC_TOKEN_AUTHENTICATION"] = "REQUIRED"
            
            self.collector = htcondor.Collector(self.config["collector_host"])
            submit_host_url = self.config["submit_host"]
            submit_host = htcondor.classad.quote(submit_host_url)
            
            # Setup security manager with token
            schedd_ads = self.collector.query(
                htcondor.AdTypes.Schedd,
                constraint=f"Name=?={submit_host}",
                projection=["Name", "MyAddress", "DaemonCoreDutyCycle", "CondorVersion"]
            )

            if not schedd_ads:
                raise ValueError(f"No scheduler found for {submit_host_url}")
            
            schedd_ad = schedd_ads[0]
            self.schedd = htcondor.Schedd(schedd_ad)

            cred_ads = self.collector.query(
                htcondor.AdTypes.Credd,
                constraint=f'Name == "{submit_host_url}"'
            )

            if not cred_ads:
                print(f"No credential daemon found for {submit_host_url}. Continuing without it.")
                self.credd = None
            else:
                cred_ad = cred_ads[0]
                print(f"Found credential daemon: {cred_ad.get('Name', 'Unknown')}")
                self.credd = htcondor.Credd(cred_ad)
                
                # Add credentials for required services
                self._add_credentials()

            logger.info("Successfully connected to HTCondor submit node")
        except Exception as e:
            logger.error(f"Failed to setup HTCondor connection: {e}")
            raise

    def submit_jobs(self, submit_config: dict) -> int:
        """Submit jobs to HTCondor using files.txt"""
        try:
            # Add credentials for required services
            self._add_credentials()

            # create the Submit object
            submit_obj = htcondor.Submit(submit_config)

            # Submit the job
            result = self.schedd.submit(submit_obj, spool=True)
            cluster_id = result.cluster()

            # Spool the files
            # this transfers files from the current machine to the submit node
            self.schedd.spool(result)

            logger.info(f"Successfully submitted job to cluster {cluster_id}")
            return cluster_id

        except Exception as e:
            logger.error(f"Failed to submit jobs: {e}")
            raise

    def monitor_jobs(self, cluster_id: int, check_interval: int = 30) -> bool:
        total_wait = 0

        """Monitor job progress"""
        try:
            while True:
                # Add credentials for required services
                self._add_credentials()

                # get job statuses
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
            self._add_credentials()
            self.schedd.retrieve(f"ClusterId == {cluster_id}")
            logger.info(f"Successfully retrieved output for cluster {cluster_id}")
        except Exception as e:
            logger.error(f"Failed to retrieve output: {e}")
            raise

    def _add_credentials(self):
        """Add credentials for required services"""
        if self.credd:
            for service in ["rdrive", "scitokens"]:
                try:
                    self.credd.add_user_service_cred(htcondor.CredType.OAuth, b"", service)
                except Exception as e:
                    print(f"Could not add credential for {service}: {e}")