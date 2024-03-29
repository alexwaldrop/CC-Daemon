import time
import logging
import threading
import sys
import os

from Config import ConfigParser
from CCDaemon.Workers import LaunchWorker, RunWorker, ReportWorker
from CCDaemon.Pipeline import PipelineStatus, PipelineError
from CCDaemon.Database import DBHelper
from PipelineQueue import PipelineQueue
from PlatformFactory import PlatformFactory
from Emailer import Emailer

class DaemonManager:

    AVAILABLE_PLATFORMS = ["Google"]

    def __init__(self, config_file, platform_type):

        # Parse config file and separate into sub-configs
        self.config_file    = config_file
        self.config         = self.__read_config()

        # Determine CC daemon type
        self.platform_type = platform_type.upper()

        ## Initialize CC daemon components ##
        # Create DBHelper
        self.db_helper = self.__init_db_helper()

        # Create PipelineQueue
        self.pipeline_queue = self.__init_pipeline_queue()

        # Create Platform factory
        self.platform_factory = self.__init_platform_factory()

        # Create ReportQueue
        self.report_queue = self.__init_report_queue()

        # Create EmailReporter
        self.email_reporter = self.__init_email_reporter()

        # Sleep periods for daemon and worker threads
        self.email_recipients   = self.config.pop("email_recipients")
        self.daemon_sleep_time  = self.config.get("daemon_sleep_time",   60)
        self.worker_sleep_time  = self.config.get("worker_sleep_time",   5)

        # Create worker threads
        self.launch_worker  = LaunchWorker(self.db_helper, self.pipeline_queue, self.platform_factory, self.worker_sleep_time)
        self.run_worker     = RunWorker(self.db_helper, self.pipeline_queue, self.worker_sleep_time)
        self.report_worker  = ReportWorker(self.db_helper, self.pipeline_queue, self.report_queue, self.platform_factory.get_platform("ReportPlatform"))

        # Stop thread
        self.stopped = False

        # Boolean for whether worker threads have started
        self.summoned = False

    def validate(self):

        # Validate report queue
        logging.info("Validating ReportQueue...")
        if not self.report_queue.is_valid():
            logging.error("ReportQueue did not pass validation!")
            exit(1)

        # Validate PlatformFactory
        logging.info("Validating PlatformFactory...")
        if not self.platform_factory.is_valid():
            logging.error("PlatformFactory did not pass validation!")
            exit(1)

        logging.info("Validating EmailReporter...")
        if not self.email_reporter.is_valid():
            logging.error("EmailReporter did not pass validation!")
            exit(1)

    def summon(self):

        # Start running all workers
        logging.info("Summoning CC-Daemon...")

        # Update potentially outdated pipeline statuses in DB
        self.__update_outdated_runs()

        self.summoned = True
        self.launch_worker.start()
        self.run_worker.start()
        self.report_worker.start()
        logging.info(
            "CC-Daemon is afoot. Tread lightly brave warrior. There are fouler things than orcs in these mines...")

        while not self.is_stopped():

            # Print status of current pipeline queue
            logging.info("\n\n%s\n\n" % self.pipeline_queue)

            # Raise any errors thrown by any worker thread
            self.launch_worker.check()
            self.run_worker.check()
            self.report_worker.check()

            # Sleep for a lil bit
            time.sleep(self.daemon_sleep_time)

    def finalize(self, err_msg=None):

        # Stop CC-Daemon from running
        self.stop()
        try:

            # Clean up platform if worker threads have started
            logging.info("Cleaning up CC-Daemon pipelines!")
            if self.summoned:
                self.clean_up()

        except BaseException, e:
            # Report any error messages
            logging.info("(CCDaemon) Unable to complete CCDaemon clean-up!")
            if e.message != "":
                logging.info("Received the following error message: %s" % e.message)

        finally:
            # Report CC-Daemon failure message
            logging.info("Notifying administrators of CCDaemon failure...")
            self.report_failure(err_msg)

    def clean_up(self):
        # Stop all pipelines and return when pipeline queue is empty

        # Stop any new pipelines from launching
        logging.info("Stopping new jobs from launching...")
        self.launch_worker.stop()

        # Cancel all pipelines currently in pipeline queue
        logging.info("Canceling all currently running jobs...")
        active_pipelines = self.pipeline_queue.get_pipelines().values()
        for active_pipeline in active_pipelines:
            # Cancel any jobs that haven't finished
            active_pipeline.cancel()

        # If run work isn't stopped, pipeline jobs can be normally dequeued
        # Wait until all pipeline jobs have been cancelled, destroyed, and registered in database
        logging.info("Waiting while RunWorker tries to clear pipeline queue...")
        while not self.run_worker.is_stopped() and not self.pipeline_queue.is_empty():
            time.sleep(5)

        if self.pipeline_queue.is_empty():
            # Report that pipeline queue is empty
            logging.info("Successfully cleared all pipelines from pipeline queue!")
        else:
            # Report that pipeline queue still contains pipelines and RunWorker has failed
            logging.info("RunWorker unable to clear all pipelines from pipeline queue!")

            # Try to destroy any remaining pipelines
            active_pipelines = self.pipeline_queue.get_pipelines().values()
            for active_pipeline in active_pipelines:
                try:
                    logging.info("Trying one last time to destroy pipeline: '%s'" % active_pipeline.get_id())
                    active_pipeline.platform.finalize()
                except BaseException, e:
                    logging.error("Unable to destroy pipeline '%s'!" % active_pipeline.get_id())
                    if e.message != "":
                        logging.error("Received following error message: %s" % e.message)

        # Otherwise just stop all threads and quit
        self.report_worker.stop()
        self.run_worker.stop()

        # Wait for everything to stop
        while not self.run_worker.is_stopped() or not self.report_worker.is_stopped():
            time.sleep(1)

    def report_failure(self, err_msg=None):
        logging.info("Emailing recipients about CC-Daemon failure...")

        # Create message body
        msg_body = "CC-Daemon has failed!"
        if err_msg is not None:
            msg_body += "\n%s" % err_msg

        # Send email notifying recipients of CCDaemon failure
        self.email_reporter.send_email(self.email_recipients, msg_body, msg_subj="FAILURE ALERT!")

    def is_stopped(self):
        with threading.Lock():
            return self.stopped

    def stop(self):
        with threading.Lock():
            self.stopped = True

    def update_pipeline_queue(self):
        # Periodically check to see if pipeline queue resource limits have changes
        try:

            # Read to see if values for pipeline queue have changes from last time
            config = self.__read_config().pop("pipeline_queue")
            max_cpus    = config["max_cpus"]
            max_loading = config["max_loading"]

            if max_cpus != self.pipeline_queue.max_cpus:
                logging.info("Updating pipeline queue CPU limit from %d to %d!" %
                             (self.pipeline_queue.max_cpus, max_cpus))
                self.pipeline_queue.set_max_cpus(max_cpus)

            if max_loading != self.pipeline_queue.load_limit:
                logging.info("Updating pipeline queue loading limit from %d to %d!" %
                             (self.pipeline_queue.load_limit, max_loading))
                self.pipeline_queue.set_max_loading(max_loading)


        except BaseException, e:
            logging.error("(CCDaemon) Unable to refresh pipeline queue from config file!")
            if e.message != "":
                logging.error("(CCDaemon) Received the following error message: %s" % e.message)

    def __read_config(self):
        # Return parsed, validated config
        exec_dir        = sys.path[0]
        config_schema   = os.path.join(exec_dir, "CCDaemon/CCDaemon.validate")
        return ConfigParser(self.config_file, config_spec=config_schema).get_config()

    def __init_db_helper(self):
        # Initialize database helper from config
        logging.info("(CCDaemon) Initializing DBHelper and connecting to database...")
        config = self.config.pop("db_helper")
        return DBHelper(username=config["username"],
                        password=config["password"],
                        database=config["database"],
                        host=config["host"],
                        mysql_driver=config["mysql_driver"])

    def __init_pipeline_queue(self):
        # Initialize pipeline queue
        logging.info("(CCDaemon) Initializing PipelineQueue...")
        config          = self.config.pop("pipeline_queue")
        max_cpus        = config["max_cpus"]
        load_limit      = config["max_loading"]
        return PipelineQueue(max_cpus, load_limit)

    def __init_platform_factory(self):
        logging.info("(CCDaemon) Initializing PlatformFactory...")
        config = self.config.pop("platform")
        if self.platform_type == "GOOGLE":
            from Google import GooglePlatform
            return PlatformFactory(config=config, platform_class=GooglePlatform)
        else:
            raise IOError("Unable to initialize platform factory! Unsupported platform type: '%s'." % self.platform_type)

    def __init_report_queue(self):
        logging.info("(CCDaemon) Initializing ReportQueue...")
        config = self.config.pop("report_queue")
        if self.platform_type == "GOOGLE":
            from Google import GoogleReportQueue
            return GoogleReportQueue(config)
        else:
            raise IOError("Unable to initialize report queue! Unsupported platform type: '%s'." % self.platform_type)

    def __init_email_reporter(self):
        logging.info("(CCDaemon) Initializing Email Reporter...")
        return Emailer(config=self.config.pop("email_reporter"))

    def __update_outdated_runs(self):
        # Function to be called on CC-Daemon startup that goes through all previous analyses
        # Pipelines with status other than IDLE, FAILED, COMPLETE should be set to FAILED
        logging.info("Updating status of outdated runs...")

        # Pipelines with these stati won't be touched
        acceptable_statuses = [PipelineStatus.FAILED, PipelineStatus.IDLE, PipelineStatus.SUCCESS]

        try:
            # Create new database session
            with self.db_helper.session_context() as session:
                # Get all pipelines in DB
                pipelines = self.db_helper.get_pipeline(session)

                # Loop through and update status of each
                for pipeline in pipelines:
                    if pipeline.status.description.upper() not in acceptable_statuses:
                        logging.info("Orphaned pipeline to be updated: %s" % pipeline.analysis_id)

                        # Set status to FAILED
                        self.db_helper.update_status(pipeline, status=PipelineStatus.FAILED)

                        # Set error to type to OTHER and indicate that orphaned pipeline was detected by daemon
                        self.db_helper.update_error_type(pipeline, error_type=PipelineError.OTHER, extra_error_msg="Orphaned pipeline updated upon daemon start!")

            logging.info("Pipeline status update complete!")

        except BaseException, e:
            logging.error("(%s) Unable to update outdated run statuses!" % self.__class__.__name__)
            if e.message != "":
                logging.error("Received the following error message: %s" % e.message)
            raise
