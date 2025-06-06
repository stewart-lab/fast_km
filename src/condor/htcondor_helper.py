import htcondor2 as htcondor
import time
import os
from typing import Any

class HTCondorHelper:
    def __init__(self, config, token_dir: str):
        """Initialize HTCondor helper with Config object and token directory"""
        self.config = config
        self.logger = config.logger
        self.token_dir = token_dir
        self._validate_config()
        self._setup_connection()

    def _validate_config(self):
        """Validate required HTCondor configuration"""
        required_attrs = [
            'collector_host',
            'submit_host',
            'docker_image',
            'request_gpus',
            'request_cpus',
            'request_memory',
            'request_disk'
        ]
        
        missing = [attr for attr in required_attrs if not hasattr(self.config, attr)]
        if missing:
            raise ValueError(f"Missing HTCondor config attributes: {', '.join(missing)}")

    def _setup_connection(self):
        """Setup connection to HTCondor submit node"""
        try:
            # Print info about the token directory
            self.logger.info(f"Token directory: {self.token_dir}")
            if os.path.exists(self.token_dir):
                token_files = os.listdir(self.token_dir)
                self.logger.info(f"Files in token directory: {token_files}")
            
            # Use explicit token file path
            token_file = os.path.join(self.token_dir, 'condor_token')
            if not os.path.exists(token_file):
                raise ValueError(f"Token file not found at {token_file}")
            
            # Set absolute token directory
            abs_token_dir = os.path.abspath(self.token_dir)
            self.logger.info(f"Using token directory: {abs_token_dir}")
            htcondor.param["SEC_TOKEN_DIRECTORY"] = abs_token_dir
            
            # Set authentication methods to use only TOKEN
            htcondor.param["SEC_CLIENT_AUTHENTICATION_METHODS"] = "TOKEN"
            htcondor.param["SEC_DEFAULT_AUTHENTICATION_METHODS"] = "TOKEN"
            
            # Force token authentication 
            htcondor.param["SEC_TOKEN_AUTHENTICATION"] = "REQUIRED"
            
            self.logger.info("HTCondor security parameters configured")
            
            # Setup collector
            self.collector = htcondor.Collector(self.config.collector_host)
            submit_host = htcondor.classad.quote(self.config.submit_host)
            
            # Query scheduler daemon
            self.logger.debug(f"Querying for schedd on {self.config.submit_host}")
            schedd_ads = self.collector.query(
                htcondor.AdTypes.Schedd,
                constraint=f"Name=?={submit_host}",
                projection=["Name", "MyAddress", "DaemonCoreDutyCycle", "CondorVersion"]
            )

            if not schedd_ads:
                raise ValueError(f"No scheduler found for {self.config.submit_host}")
            
            schedd_ad = schedd_ads[0]
            self.logger.debug(f"Found scheduler: {schedd_ad.get('Name', 'Unknown')}")
            self.schedd = htcondor.Schedd(schedd_ad)

            # Test connection to schedd
            self.logger.debug("Testing connection to schedd...")
            try:
                test_query = self.schedd.query(constraint="False", projection=["ClusterId"])
                self.logger.debug(f"Successfully connected to schedd, got {len(test_query)} results")
            except Exception as e:
                self.logger.error(f"Failed to connect to schedd: {str(e)}")
                raise
            
            # Query credential daemon
            self.logger.debug(f"Querying for credential daemon on {self.config.submit_host}")
            cred_ads = self.collector.query(
                htcondor.AdTypes.Credd,
                constraint=f'Name == "{self.config.submit_host}"'
            )

            if not cred_ads:
                self.logger.warning(f"No credential daemon found for {self.config.submit_host}. Continuing without it.")
                self.credd = None
            else:
                cred_ad = cred_ads[0]
                self.logger.debug(f"Found credential daemon: {cred_ad.get('Name', 'Unknown')}")
                self.credd = htcondor.Credd(cred_ad)
                
                # Add credentials for required services
                if self.credd:
                    for service in ["rdrive", "scitokens"]:
                        try:
                            self.credd.add_user_service_cred(htcondor.CredType.OAuth, b"", service)
                            self.logger.debug(f"Added credential for service: {service}")
                        except Exception as e:
                            self.logger.warning(f"Could not add credential for {service}: {e}")

            self.logger.info("Successfully connected to HTCondor submit node")
        except Exception as e:
            self.logger.error(f"Failed to setup HTCondor connection: {e}")
            raise

    def submit_jobs(self, files_txt_path: str) -> int:
        """Submit jobs to HTCondor using files.txt"""
        try:
            abs_files_txt = os.path.abspath(files_txt_path)
            
            # Read the files list
            with open(abs_files_txt, 'r') as f:
                files = [line.strip() for line in f if line.strip()]
            
            self.logger.debug(f"Submitting jobs for {len(files)} files")

            # Create submit description
            submit_desc = htcondor.Submit({
                "universe": "docker",
                "docker_image": self.config.docker_image,
                "docker_pull_policy": "missing",
                "executable": "./run.sh",
                "should_transfer_files": "YES",
                "when_to_transfer_output": "ON_EXIT",
                "transfer_input_files": ".",
                "output": "output/std_out.out",
                "error": "output/std_err.err",
                "log": "run_$(Cluster).log",
                "request_gpus": self.config.request_gpus,
                "request_cpus": self.config.request_cpus,
                "request_memory": self.config.request_memory,
                "request_disk": self.config.request_disk,
                "requirements": "(CUDACapability >= 8.0)",
                "stream_error": "True",
                "stream_output": "True",
                "transfer_output_files": "output",
                "+WantGPULab": "true",
                "+GPUJobLength": '"short"',
            })

            # Add credentials for required services again before submission
            if self.credd:
                for service in ["rdrive", "scitokens"]:
                    try:
                        self.credd.add_user_service_cred(htcondor.CredType.OAuth, b"", service)
                    except Exception as e:
                        self.logger.warning(f"Could not add credential for {service}: {e}")
            
            # Submit jobs
            self.logger.debug("Submitting job batch to HTCondor")
            
            result = self.schedd.submit(submit_desc, spool=True)
            self.schedd.spool(result)
            self.logger.info(f"Successfully submitted {len(files)} jobs in cluster {result.cluster()}")
            return result.cluster()

        except Exception as e:
            self.logger.error(f"Failed to submit jobs: {e}")
            raise

    def monitor_jobs(self, cluster_id: int, check_interval: int = 30) -> bool:
        """Monitor job progress"""
        try:
            total_wait = 0
            
            while True:
                try:
                    # Add credentials for required services before querying
                    if self.credd:
                        for service in ["rdrive", "scitokens"]:
                            try:
                                self.credd.add_user_service_cred(htcondor.CredType.OAuth, b"", service)
                            except Exception as e:
                                self.logger.warning(f"Could not add credential for {service}: {e}")

                        self.logger.debug(f"Querying status for cluster {cluster_id}")
                        ads = self.schedd.query(
                            constraint=f"ClusterId == {cluster_id}",
                            projection=["ProcID", "JobStatus", "HoldReason", "HoldReasonCode"]
                        )
                
                    if not ads:
                        self.logger.warning(f"No jobs found for cluster {cluster_id}")
                        return False
                    
                    # Check if all jobs completed
                    completed_jobs = [ad for ad in ads if ad.get("JobStatus") == 4]
                    if len(completed_jobs) == len(ads):
                        self.logger.info(f"All jobs in cluster {cluster_id} completed successfully")
                        return True
                    
                    # Check for held jobs
                    held_jobs = [ad for ad in ads if ad.get("JobStatus") == 5]
                    if held_jobs:
                        for job in held_jobs[:3]:  # Log first 3 held jobs
                            hold_reason = job.get("HoldReason", "Unknown")
                            hold_code = job.get("HoldReasonCode", 0)
                            proc_id = job.get("ProcID", "Unknown")
                            self.logger.warning(f"Job {cluster_id}.{proc_id} held: {hold_reason} (code: {hold_code})")
                    
                    # Log status counts
                    status_counts = {}
                    for ad in ads:
                        status = ad.get("JobStatus", 0)
                        status_counts[status] = status_counts.get(status, 0) + 1
                    
                    status_desc = {
                        1: "Idle", 2: "Running", 3: "Removed",
                        4: "Completed", 5: "Held", 6: "Transferring Output",
                        7: "Suspended"
                    }
                    
                    status_msg = ", ".join(f"{status_desc.get(k, f'Unknown({k})')}: {v}" 
                                           for k, v in status_counts.items())
                    self.logger.info(f"Cluster {cluster_id} status: {status_msg}. Total wait time: {total_wait} seconds")
                    
                    # Retrieve intermediate output files every minute
                    self.logger.info(f"Attempting to retrieve intermediate output files for cluster {cluster_id}")
                    try:
                        # Add timeout to prevent hanging
                        retrieve_start = time.time()
                        self.logger.debug(f"Starting retrieve operation at {retrieve_start}")
                        
                        # Set a timeout for the retrieve operation
                        max_retrieve_time = 60  # 60 seconds timeout
                        
                        # Use a separate thread for the retrieve operation
                        import threading
                        retrieve_success = [False]
                        retrieve_error = [None]
                        
                        def retrieve_thread():
                            try:
                                # Add credentials for required services
                                if self.credd:
                                    for service in ["rdrive", "scitokens"]:
                                        try:
                                            self.credd.add_user_service_cred(htcondor.CredType.OAuth, b"", service)
                                        except Exception as e:
                                            self.logger.warning(f"Could not add credential for {service}: {e}")
                                
                                self.schedd.retrieve(f"ClusterId == {cluster_id}")
                                retrieve_success[0] = True
                            except Exception as e:
                                retrieve_error[0] = e
                        
                        thread = threading.Thread(target=retrieve_thread)
                        thread.daemon = True
                        thread.start()
                        
                        # Wait for the thread with timeout
                        thread.join(max_retrieve_time)
                        
                        if thread.is_alive():
                            self.logger.warning(f"Retrieve operation timed out after {max_retrieve_time} seconds")
                            # Continue with monitoring even if retrieve times out
                        elif retrieve_success[0]:
                            self.logger.info(f"Successfully retrieved intermediate output files for cluster {cluster_id}")
                        else:
                            self.logger.warning(f"Failed to retrieve intermediate output files: {retrieve_error[0]}")
                            
                        retrieve_end = time.time()
                        self.logger.debug(f"Retrieve operation took {retrieve_end - retrieve_start:.2f} seconds")
                        
                    except Exception as retrieve_err:
                        self.logger.warning(f"Warning: Failed to retrieve intermediate output files: {retrieve_err}")
                    
                    # Check for timeout on held jobs
                    if total_wait >= 120:
                        job_is_idle = status_counts.get(1, 0) > 0
                        job_is_running = status_counts.get(2, 0) > 0
                        if not job_is_idle and not job_is_running:
                            raise Exception(f"Cluster {cluster_id} timed out (job was held too long)")

                    time.sleep(check_interval)
                    total_wait += check_interval
                    
                except Exception as query_error:
                    self.logger.warning(f"Query error: {query_error}")
                    time.sleep(check_interval)
                    total_wait += check_interval

        except Exception as e:
            self.logger.error(f"Error monitoring jobs: {e}")
            raise

    def retrieve_output(self, cluster_id: int):
        """Retrieve output files from completed jobs"""
        try:
            # Add credentials for required services
            if self.credd:
                for service in ["rdrive", "scitokens"]:
                    try:
                        self.credd.add_user_service_cred(htcondor.CredType.OAuth, b"", service)
                    except Exception as e:
                        self.logger.warning(f"Could not add credential for {service}: {e}")
            
            self.schedd.retrieve(f"ClusterId == {cluster_id}")
            self.logger.info(f"Successfully retrieved output for cluster {cluster_id}")
        except Exception as e:
            self.logger.error(f"Failed to retrieve output: {e}")
            raise

    def cleanup(self, cluster_id: int):
        """Clean up after job completion"""
        try:
            # Add credentials for required services
            if self.credd:
                for service in ["rdrive", "scitokens"]:
                    try:
                        self.credd.add_user_service_cred(htcondor.CredType.OAuth, b"", service)
                    except Exception as e:
                        self.logger.warning(f"Could not add credential for {service}: {e}")
            
            # Remove all jobs from the queue
            self.logger.info(f"Removing all jobs in cluster {cluster_id}")
            remove_result = self.schedd.act(htcondor.JobAction.Remove, f"ClusterId == {cluster_id}")
            self.logger.info(f"Remove result: {remove_result}")
                
            self.logger.info(f"Cleanup completed for cluster {cluster_id}")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
            raise