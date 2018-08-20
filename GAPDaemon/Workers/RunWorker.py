import logging
from datetime import datetime

from GAPDaemon.Workers import StatusWorker
from GAPDaemon.Pipeline import PipelineStatus, PipelineError

class RunWorker(StatusWorker):
    # Main class for loading idle pipelines from database
    def __init__(self, db_helper, pipeline_queue, sleep_time=2):
        super(RunWorker, self).__init__(db_helper, pipeline_queue, sleep_time)

    def task(self, session):

        # Get list of currently active pipelines
        active_pipelines = self.pipeline_queue.get_pipelines().values()

        for active_pipeline in active_pipelines:

            # Get pipeline record from database
            db_pipeline = self.db_helper.get_pipeline(session, pipeline_id=active_pipeline.get_id())

            # Get current status of pipeline runner
            curr_status = active_pipeline.get_status()

            # For all pipelines that haven't finished
            if curr_status in [PipelineStatus.READY, PipelineStatus.LOADING, PipelineStatus.RUNNING]:

                # Cancel pipeline if pipeline has been set to cancelled in the database
                if db_pipeline.status.description.upper() == PipelineStatus.CANCELLING:
                    logging.error("(RunWorker) Pipeline '%s' has been cancelled from the database by the user!" % active_pipeline.get_id())
                    active_pipeline.cancel()
                    continue

                # Sync database to reflect current status of pipeline runner
                self.sync_run_status(db_pipeline, curr_status)

                # Check to see if pipeline has exceeded it's runtime
                create_time = active_pipeline.get_start_time()
                max_runtime = db_pipeline.analysis_type.max_run_time
                if self.__time_elapsed(start=create_time, end=datetime.now()) > max_runtime:
                    # Cancel the job if it's exceeded it's time limit
                    logging.error("(RunWorker) Pipeline '%s' has exceeded maximum runtime (%d hours)!" % (active_pipeline.get_id(), max_runtime))
                    active_pipeline.cancel()

            # Update run results for finished pipelines
            elif curr_status == PipelineStatus.FINISHED:

                # Report that pipeline was marked as finished
                logging.debug("(RunWorker) Pipeline finished: %s" % active_pipeline.get_id())

                # Record pipeline runtime in database
                start_time              = active_pipeline.get_start_time()
                end_time                = active_pipeline.get_end_time()
                db_pipeline.run_time    = self.__time_elapsed(start=start_time, end=end_time)

                # Record pipeline success status in database
                curr_err_type           = active_pipeline.get_err_type()
                curr_err_msg            = active_pipeline.get_err_msg()

                # Put dummy pipeline report indicating failure
                self.sync_run_status(db_pipeline, curr_status=PipelineStatus.FAILED)

                # Put dummy pipeline report indicating type of error that caused failure
                self.sync_error_status(db_pipeline, curr_err_type, curr_err_msg)

                # Remove pipeline from queue
                logging.debug("Removing pipeline '%s' from pipeline queue!" % active_pipeline.get_id())
                self.pipeline_queue.remove_pipeline(active_pipeline.get_id())

            # Commit any changes to database
            session.commit()

    def sync_run_status(self, pipeline, curr_status):

        # Sync pipeline status in database with current pipeline_runner status
        if pipeline.status.description != curr_status:

            # Update database record to be current with pipeline runner
            self.db_helper.update_status(pipeline, status=curr_status)

    def sync_error_status(self, pipeline, curr_err_type, curr_err_msg):

        if curr_err_type == PipelineError.NONE:
            # Case: Pipeline successfully completed.
            # Post a dummy report until GAP run report is processed.
            logging.debug("(RunWorker) Pipeline '%s' was successful! Awaiting report from queue..." % pipeline.analysis_id)
            self.db_helper.update_error_type(pipeline, error_type=PipelineError.REPORT)

        elif curr_err_type == PipelineError.CANCEL:
            # Case: Run cancelled by user due to timeout or upon daemon exit
            # Post report because any GAP report will attribute errors to CTRL+C interrupt instead of cancellation
            logging.debug("(RunWorker) Pipeline '%s' failed due to cancellation!" % pipeline.analysis_id)
            self.db_helper.update_error_type(pipeline, error_type=PipelineError.CANCEL)

        else:
            # Case: Run fail or Load fail. Write error report as is.
            logging.debug("(RunWorker) Pipeline '%s' failed due to loading or runtime error!" % pipeline.analysis_id)
            self.db_helper.update_error_type(pipeline, error_type=curr_err_type, extra_error_msg=curr_err_msg)

    @staticmethod
    def __time_elapsed(start, end):
        # Return the number of hours that have passed between two datetime intervals
        diff = end - start
        days, seconds = diff.days, diff.seconds
        hours = days * 24 + (seconds / 3600.0)
        return hours