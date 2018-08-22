import json
import logging
import os
import subprocess as sp
import sys
from StringIO import StringIO

from configobj import ConfigObj

from CCDaemon.Platform import Platform
from GoogleProcessor import GoogleProcessor

class GooglePlatform(Platform):
    def __init__(self, name, config):
        # Call super constructor from Platform
        super(GooglePlatform, self).__init__(name, config)

        # Get google access fields from JSON file
        self.key_file       = self.config["service_account_key_file"]
        self.service_acct   = self.__get_key_field("client_email")
        self.config["service_acct"] = self.service_acct

        self.google_project = self.__get_key_field("project_id")
        self.config["google_project"] = self.google_project

        # Get Google compute zone from config
        self.zone = self.config["zone"]

        # Use authentication key file to gain access to google cloud project using Oauth2 authentication
        self.authenticate()

        # Set platform type
        self.platform_type = "Google"

    def authenticate(self):

        logging.info("Authenticating to the Google Cloud.")

        if not os.path.exists(self.key_file):
            logging.error("Authentication key was not found!")
            exit(1)

        cmd = "gcloud auth activate-service-account --key-file %s" % self.key_file
        with open(os.devnull, "w") as devnull:
            proc = sp.Popen(cmd, stdout=devnull, stderr=sp.PIPE, shell=True)

        if proc.wait() != 0:
            out, err = proc.communicate()
            logging.error("Authentication to Google Cloud failed!")
            logging.error("Received following error: %s" % err)
            exit(1)

        logging.info("Authentication to Google Cloud was successful.")

    def preprocess_configs(self, cc_config_strings):
        # Modify platform config to point to where startup-script and key-files will be located
        plat_config = ConfigObj(StringIO(cc_config_strings["platform"]))

        # Add key file path to platform config
        self.workspace["key_file"] = os.path.join(self.wrk_dir,"google_key_file.json")
        plat_config["service_account_key_file"] = self.workspace["key_file"]

        # Replace existing platform config string with updated config string
        cc_config_strings["platform"] = "\n".join(plat_config.write())

        # Add key file data to config_strings so it will be uploaded to instance
        with open(self.key_file, "r") as fh:
                key_string = fh.read()
        cc_config_strings["key_file"] = key_string

        return cc_config_strings

    def upload_file(self, src_path, dest_path):
        cmd = "gcloud compute scp %s gap@%s:%s --zone %s" % (src_path, self.processor.name, dest_path, self.zone)
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()
        if proc.returncode != 0:
            logging.error("(%s) Unable to upload file to platform: %s!" % (self.name, src_path))
            raise RuntimeError("Unable to upload config file to platform!")

    def init_processor(self):
        # Initialize and return the main processor needed to load/manage the platform
        logging.info("Creating CloudConductor runner platform instance...")

        # Get name, nr_cpus, mem and instantiate main instance object
        name        = self.__format_instance_name("Runner-%s" % str(self.name[:20]))
        return GoogleProcessor(name, self.nr_cpus, self.mem, **self.config)

    def path_exists(self, path, job_name=None):
        # Determine if a path exists either locally on platform or remotely
        job_name = "check_exists_%s" % self.generate_unique_id() if job_name is None else job_name
        if ":" in path:
            # Check if path exists on google bucket storage
            cmd         = "gsutil ls %s" % path
            proc        = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
            out, err    = proc.communicate()
            return len(err) == 0
        else:
            # Check if file exists locally on main instance
            cmd     = "ls %s" % path
            self.processor.run(job_name, cmd)
            try:
                out, err = self.processor.wait_process(job_name)
                return len(err) == 0
            except RuntimeError:
                return False
            except:
                logging.error("Unable to check path existence: %s" % path)
                raise

    def transfer(self, src_path, dest_dir, dest_file=None, log_transfer=True, job_name=None, wait=False):
        # Transfer a remote file from src_path to a local directory dest_dir
        # Log the transfer unless otherwise specified

        # Create job name
        job_name        = "transfer_%s" % self.generate_unique_id() if job_name is None else job_name

        # Google cloud options for fast transfer
        options_fast    = '-m -o "GSUtil:sliced_object_download_max_components=200"'

        # Specify whether to log transfer
        log_flag        = "!LOG3!" if log_transfer else ""

        # Specify destination of transfer
        dest_dir        = self.standardize_dir(dest_dir)
        dest_path       = dest_dir if dest_file is None else os.path.join(dest_dir, dest_file)

        # Run command to copy file
        cmd             = "gsutil %s cp -r %s %s %s" % (options_fast, src_path, dest_path, log_flag)
        self.processor.run(job_name, cmd)
        if wait:
            self.processor.wait_process(job_name)

    def mkdir(self, dir_path, job_name=None, wait=False):
        # Makes a directory if it doesn't already exists
        # Standardize dir_path
        dir_path = self.standardize_dir(dir_path)
        job_name = "mkdir_%s" % self.generate_unique_id() if job_name is None else job_name

        if ":" in dir_path:
            # Make bucket if it doesn't already exist on google cloud
            bucket = "/".join(dir_path.split("/")[0:3]) + "/"
            if not self.path_exists(bucket):
                logging.debug("Creating final output bucket: %s" % bucket)
                bucket_job_name = "mk_bucket_%s" % bucket
                region      = "-".join(self.zone.split("-")[:-1])
                cmd         = "gsutil mb -p %s -c regional -l %s %s" % (self.google_project, region, bucket)
                self.run_command(bucket_job_name, cmd)
            # Generate command to add dummy file to bucket directory and delete local copy
            logging.debug("Creating dummy output file in final output dir on google storage...")
            cmd = "touch dummy.txt ; gsutil cp dummy.txt %s" % dir_path
        else:
            # Generate command to make directory locally on the main instance
            cmd = "sudo mkdir -p %s" % dir_path

        # Run command
        self.run_command(job_name, cmd)

    def clean_up(self):
        logging.info("Cleaning up Google Cloud Platform.")

        # Remove dummy.txt from final output bucket
        try:
            cmd         = "gsutil rm %s" % os.path.join(self.final_output_dir,"dummy.txt")
            proc        = sp.Popen(cmd, stderr=sp.PIPE, stdout=sp.PIPE, shell=True)
            proc.communicate()
        except:
            logging.warning("(%s) Could not remove dummy input file on google cloud!")

        # Destroy main processor
        try:
            if self.processor is not None:
                self.processor.destroy()
        except RuntimeError:
            logging.warning("(%s) Could not destroy instance!" % self.processor.name())

        logging.info("Clean up complete!")

    def define_config_schema(self):
        # Return path to config spec file used to validate platform config
        exec_dir = sys.path[0]
        return os.path.join(exec_dir, "Google/GooglePlatform.validate")

    ####### PRIVATE UTILITY METHODS
    def __get_key_field(self, field_name):
        # Parse JSON service account key file and return email address associated with account
        logging.info("Extracting %s from JSON key file." % field_name)

        if not os.path.exists(self.key_file):
            logging.error("Google authentication key file not found: %s!" % self.key_file)
            raise IOError("Google authentication key file not found!")

        # Parse json into dictionary
        with open(self.key_file) as kf:
            key_data = json.load(kf)

        # Check to make sure correct key is present in dictionary
        if field_name not in key_data:
            logging.error(
                "'%s' field missing from authentication key file: %s. Check to make sure key exists in file or that file is valid google key file!"
                % (field_name, self.key_file))
            raise IOError("Info field not found in Google key file!")
        return key_data[field_name]

    @staticmethod
    def __format_instance_name(instance_name):
        # Ensures that instance name conforms to google cloud formatting specs
        old_instance_name = instance_name
        instance_name = instance_name.replace("_", "-")
        instance_name = instance_name.replace(".", "-")
        instance_name = instance_name.lower()

        # Declare if name of instance has changed
        if old_instance_name != instance_name:
            logging.warn("Modified instance name from %s to %s for compatibility!" % (old_instance_name, instance_name))

        return instance_name
