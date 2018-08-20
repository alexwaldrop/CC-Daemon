import os
import threading
import logging
from datetime import datetime

from CCDaemon.Pipeline import PipelineError, PipelineStatus

class PipelineRunner(threading.Thread):

    def __init__(self, pipeline, config_file_strings, platform):
        super(PipelineRunner, self).__init__()

        # Get data from pipeline DB record
        self.id     = pipeline.analysis_id
        self.name   = pipeline.name

        # Platform for running pipeline
        self.platform           = platform
        self.final_output_dir   = os.path.join(pipeline.final_output_dir, str(pipeline.analysis_id))
        self.platform.set_final_output_dir(self.final_output_dir)

        # Get string representations of GAP config files
        self.config_file_strings = config_file_strings

        # Initialize resource requirement variables
        self.cpus       = pipeline.analysis_type.cpus
        self.mem        = pipeline.analysis_type.mem
        self.disk_space = pipeline.analysis_type.disk_space

        # Initialize running time variables
        self.max_run_time   = pipeline.analysis_type.max_run_time
        self.create_time    = datetime.now()
        self.start_time     = None
        self.end_time       = None

        # PipelineRunner status variable
        self.status         = PipelineStatus.READY
        self.status_lock    = threading.Lock()

        # Error reporting variables
        self.err_msg    = ""
        self.err_type   = PipelineError.NONE

        # Run as a daemon so thread will quit upon error in main program
        self.daemon = True

    ##### Core Functions #####
    def run(self):
        # Load pipeline platform and run pipeline using GAP

        # Set run start time
        self.start_time = datetime.now()

        try:

            # Launch new platform and load all resources necessary to run GAP
            self.set_status(PipelineStatus.LOADING)
            self.platform.launch(cc_config_files=self.config_file_strings)

            # Exit run if pipeline cancelled by user
            if self.get_status() == PipelineStatus.CANCELLING:
                raise

            # Run GAP
            self.set_status(PipelineStatus.RUNNING)
            self.platform.run_cc()

            # Notify successful completion
            logging.info("(PipelineRunner %s) Pipeline completed successfully!" % self.id)

        except BaseException, e:

            # Log that pipeline failed during runtime
            logging.error("(PipelineRunner %s) Pipeline failed!" % self.id)
            curr_status = self.get_status()

            if curr_status == PipelineStatus.LOADING:
                # Indicate that error occurred while loading platform
                self.err_type   = PipelineError.LOAD
                self.err_msg    = "Check GAP daemon runlog!"

                # Add additional error messages if any
                if e.message != "":
                    logging.error("Recieved the following error: %s" % e.message)
                    self.err_msg += "\n Received the following error message: %s" % e.message

            elif curr_status == PipelineStatus.RUNNING:
                # Indicate that error occurred while running GAP
                self.err_type   = PipelineError.RUN
                self.err_msg    = "Check GAP error log in %s!" % self.final_output_dir

                # Add additional error messages if any
                if e.message != "":
                    logging.error("Recieved the following error: %s" % e.message)
                    self.err_msg += "\n Received the following error message: %s" % e.message

        finally:

            # Mark time of completion
            self.end_time = datetime.now()

            # Clean-up the platform
            self.finalize()

    def cancel(self):
        # Halt and destroy pipeline during runtime
        curr_status = self.get_status()

        # Don't do anything if pipeline has already finished
        if curr_status in [PipelineStatus.DESTROYING, PipelineStatus.FINISHED, PipelineStatus.CANCELLING]:
            return

        # Set pipeline to cancelling and stop any currently running jobs
        logging.error("(PipelineRunner %s) Pipeline cancelled!" % self.id)

        self.set_status(PipelineStatus.CANCELLING)
        self.err_type = PipelineError.CANCEL

        if curr_status == PipelineStatus.RUNNING:
            # Gracefully kill GAP if currently running
            self.platform.cancel_cc()

        elif curr_status == PipelineStatus.LOADING:
            # Gracefully stop platform if loading
            self.platform.cancel_launch()

    def finalize(self):

        # Do nothing if PipelineRunner is in the process of destroying itself or is already destroyed
        if self.get_status() in [PipelineStatus.DESTROYING, PipelineStatus.FINISHED]:
            return

        try:
            # Destroy platform
            logging.info("(PipelineRunner %s) finalizing pipeline runner!" % self.id)
            self.set_status(PipelineStatus.DESTROYING)
            self.platform.finalize()

        except BaseException, e:
            logging.error("(PipelineRunner %s) Error finalizing pipeline!" % self.id)
            if e.message != "":
                logging.error("PipelineRunner Received the following error: %s" % e.message)

        finally:
            # Mark as destroyed
            self.set_status(PipelineStatus.FINISHED)

    ##### Getters and Setters #####
    def get_status(self):
        with self.status_lock:
            return self.status

    def set_status(self, status):
        with self.status_lock:
            self.status = status

    def get_err_type(self):
        with self.status_lock:
            return self.err_type

    def get_err_msg(self):
        with self.status_lock:
            return self.err_msg

    def get_create_time(self):
        return self.create_time

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def get_id(self):
        return self.id

    def get_cpus(self):
        return self.cpus

    def get_mem(self):
        return self.mem

    def get_disk_space(self):
        return self.disk_space