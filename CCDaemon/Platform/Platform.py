import logging
import os
import abc
import uuid
import time

from Config import Validatable

class Platform(Validatable):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name, config):

        # Call Validatable super constructor to parse config
        super(Platform, self).__init__(config)

        # Platform name
        self.name = name

        # Init essential platform variables from config
        self.nr_cpus            = self.config.pop("nr_cpus")
        self.mem                = self.config.pop("mem")
        self.cc_git_url         = self.config["cc_url"]
        self.wrk_dir            = self.standardize_dir(self.config["wrk_dir"])

        # Define workspace filenames
        self.workspace = self.__define_workspace(self.wrk_dir)

        # Define workspace directory names
        self.final_output_dir   = None

        # Boolean for whether main processor has been initialized
        self.launched = False

        # Add variable for platform type
        self.platform_type = "Base"

        # Main platform processor
        self.processor = None

    def launch(self, cc_config_files, commit_id=None):

        # Loads platform capable of running pipeline
        logging.info("(%s) Creating platform..." % self.name)
        #self.processor  = self.create_processor()

        # Initialize processor object
        self.processor  = self.init_processor()

        # Create processor object
        self.processor.create()

        # Specify that processor has successfully been created
        self.launched   = True

        # Create working and log directories
        logging.info("(%s) Creating working directory: %s!" % (self.name, self.wrk_dir))
        self.mkdir(self.wrk_dir)

        logging.info("(%s) Creating log directory: %s!" % (self.name, self.workspace["log_dir"]))
        self.mkdir(self.workspace["log_dir"])
        self.processor.set_log_dir(self.workspace["log_dir"])

        logging.info("(%s) Creating CC directory: %s!" % (self.name, self.workspace["cc_dir"]))
        self.mkdir(self.workspace["cc_dir"])

        # Grant all permissions to working directory
        logging.info("(%s) Granting write permissions!" % self.name)
        cmd = "sudo chmod -R 777 %s" % self.wrk_dir
        self.run_command("grant_permissions", cmd)

        # Create final output directory and wait to make sure it was actually created
        if not self.path_exists(self.final_output_dir):
            logging.info("(%s) Creating output directory: %s" % (self.name, self.final_output_dir))
            self.mkdir(self.final_output_dir)
            self.processor.wait()

        # Install CC freshly from GitHub
        logging.info("(%s) Downloading CloudConductor!" % self.name)
        cmd = "sudo git clone %s %s !LOG3!" % (self.cc_git_url, self.workspace["cc_dir"])
        self.run_command("download_cc", cmd)

        # Revert to desired commit if specified
        if commit_id is not None:
            logging.info("(%s) Reverting CloudConductor to commid id: %s" % (self.name, commit_id))
            cmd = "cd %s ; sudo git reset --hard %s" % (self.workspace["cc_dir"], commit_id)
            self.run_command("git_reset_cc", cmd)

        # Make any platform-specific modifications to config files
        logging.info("(%s) Preprocessing config files!" % self.name)
        files_to_upload = self.preprocess_configs(cc_config_files)

        # Transfer CC config files
        for file_type in files_to_upload:
            file_string = files_to_upload[file_type]
            if file_string is not None:
                # Upload config file to platform
                logging.info("(%s) Uploading '%s' config to platform..." % (self.name, file_type))
                dest_path = self.workspace[file_type]
                self.upload_config(file_string, dest_path)
        logging.info("(%s) Platform successfully loaded!" % self.name)

    def upload_config(self, config_string, dest_path):

        # Write string to local file
        local_filename = "/tmp/upload.%s.txt" % self.generate_unique_id()
        with open(local_filename, "w") as fh:
            fh.write(config_string)

        # Upload file
        self.upload_file(local_filename, dest_path)

        # Remove local file
        os.remove(local_filename)

    def get_cc_version(self):
        cmd = "cd {0} ; git log -1 --pretty=%H".format(self.workspace["cc_dir"])
        out, err = self.run_command("get_cc_version", cmd)
        if len(err) != 0:
            logging.error("Unable to determine git commit version of CloudConductor! Received the following error msg:\n%s" % err)
            raise RuntimeError("Unable to determine git commit version of CloudConductor!")
        return out.strip()

    def preprocess_configs(self, cc_config_strings):
        # Empty function containing any platform specific methods for make any modifications to config files
        # E.g. - GooglePlatform config needs to point to a startup-script that will be created on the fly
        return cc_config_strings

    def run_cc(self):
        # Run CC using input files loaded onto platform
        cmd = "cd %s ; %s --input %s --name %s --pipeline_config %s --res_kit_config %s --plat_config %s --plat_name %s -o %s -vvv !LOG3!" % (
            self.workspace["cc_dir"], self.workspace["cc_exec"], self.workspace["sample_sheet"], self.name, self.workspace["graph"],
            self.workspace["resource_kit"], self.workspace["platform"], self.platform_type, self.final_output_dir)
        return self.run_command("cc", cmd)

    def cancel_cc(self):
        # Gracefully exit CC run
        # Create and upload script to stop CC to instance
        stop_script = "sudo kill -INT $(pgrep -nf 'CloudConductor/CloudConductor')"
        stop_script_path = os.path.join(self.wrk_dir, "stop_cc.sh")
        self.upload_config(config_string=stop_script, dest_path=stop_script_path)

        cmd = "bash %s" % stop_script_path
        self.run_command(job_name="stop_cc", cmd=cmd)

    def cancel_launch(self, timeout=500):
        # Gracefully exit launching platform

        # Wait for platform to be created if in the middle of launch
        elapsed = 0
        while self.processor is None and elapsed < timeout:
            time.sleep(1)
            elapsed += 1

        # Stop platform after timeout period
        self.processor.stop()

    def return_output(self, job_name, output_path, sub_dir=None, dest_file=None, log_transfer=True):

        logging.info("Returning output file: %s" % output_path)

        # Setup subdirectory within final output directory, if necessary final output directory
        if sub_dir is not None:
            dest_dir = os.path.join(self.final_output_dir, sub_dir) + "/"
            self.mkdir(dest_dir)
        else:
            dest_dir = self.final_output_dir

        # Transfer output file
        self.transfer(src_path=output_path,
                      dest_dir=dest_dir,
                      dest_file=dest_file,
                      log_transfer=log_transfer,
                      job_name=job_name)

        # Return the new path
        if dest_file is None:
            return self.standardize_dir(dest_dir) + os.path.basename(output_path)
        else:
            return self.standardize_dir(dest_dir) + dest_file

    def return_logs(self):

        # Transfer the log directory as final output
        log_file = self.workspace["log_dir"]
        self.return_output(job_name="return_logs", output_path=log_file, log_transfer=False)

        # Wait for log transfer to complete
        self.processor.wait_process("return_logs")

    def run_command(self, job_name, cmd):
        # Run a command on the platform processor
        self.processor.run(job_name, cmd)
        out, err = self.processor.wait_process(job_name)
        return out, err

    def finalize(self):

        # Copy the logs to the bucket, if platform was launched
        try:
            if self.launched:
                # Unlock processor so output can be returned
                self.processor.unlock()
                # Return logs to final output dir
                self.return_logs()
        except BaseException as e:
            logging.error("Could not return the logs to the output directory. "
                          "The following error appeared: %s" % str(e))

        # Clean up the platform
        self.clean_up()

    def set_final_output_dir(self, final_output_dir):
        self.final_output_dir = self.standardize_dir(final_output_dir)

    def __define_workspace(self, wrk_dir):
        files = {}

        wrk_dir             = self.standardize_dir(wrk_dir)

        # Init runtime logfile name
        files["log_dir"]    = self.standardize_dir(os.path.join(wrk_dir, "daemon_log"))

        # Init CC folder, executable names
        files["cc_dir"]    = self.standardize_dir(os.path.join(wrk_dir, "CloudConductor"))
        files["cc_exec"]   = os.path.join(files["cc_dir"], "CloudConductor")

        # Init config file names
        files["graph"]          = os.path.join(wrk_dir, "graph.%s.config" % self.name)
        files["resource_kit"]   = os.path.join(wrk_dir, "resource.%s.kit.config" % self.name)
        files["platform"]       = os.path.join(wrk_dir, "platform.%s.config" % self.name)
        files["sample_sheet"]   = os.path.join(wrk_dir, "input.%s.json" % self.name)

        return files

    ####### ABSTRACT VALIDATABLE METHODS TO BE INHERITED BY INHERITING CLASSES

    def is_valid(self):
        logging.info("Validating platform...")

        # Boolean for whether platform is valid
        is_valid = False

        try:
            # Test creating a processor
            self.processor = self.init_processor()
            self.processor.create()

            # Testing whether CC can be downloaded to processor
            self.mkdir(self.wrk_dir)
            self.mkdir(self.workspace["cc_dir"])

            # Grant all permissions to working directory
            cmd = "sudo chmod -R 777 %s" % self.wrk_dir
            self.run_command("grant_permissions", cmd)

            # Install CC freshly from GitHub
            cmd = "sudo git clone %s %s" % (self.cc_git_url, self.workspace["cc_dir"])
            self.run_command("download_cc", cmd)

            is_valid = True

        except BaseException, e:
            logging.error("Invalid platform!")
            if e.message != "":
                logging.error("Received following error:%s" % e.message)

        finally:
            # Clean up the platform
            self.clean_up()
            return is_valid

    def define_config_schema(self):
        return None

    ####### ABSTRACT PLATFORM METHODS TO BE IMPLEMENTED BY INHERITING CLASSES

    @abc.abstractmethod
    def upload_file(self, src_path, dest_path):
        # Upload a file from local file system to computing platform
        pass

    @abc.abstractmethod
    def clean_up(self):
        pass

    @abc.abstractmethod
    def init_processor(self):
        # Initialize and return the main processor needed to load/manage the platform
        pass

    @abc.abstractmethod
    def path_exists(self, path):
        # Determine if a path exists either locally on platform or remotely
        pass

    @abc.abstractmethod
    def transfer(self, src_path, dest_dir, dest_file=None, log_transfer=True, job_name=None):
        # Transfer a remote file from src_path to a local directory dest_dir
        # Log the transfer unless otherwise specified
        pass

    @abc.abstractmethod
    def mkdir(self, dir_path):
        # Make a directory if it doesn't already exists
        pass

    ####### PRIVATE UTILITY METHODS
    @staticmethod
    def generate_unique_id(id_len=6):
        return str(uuid.uuid4())[0:id_len]

    @staticmethod
    def standardize_dir(dir_path):
        # Makes directory names uniform to include a single '/' at the end
        return dir_path.rstrip("/") + "/"
