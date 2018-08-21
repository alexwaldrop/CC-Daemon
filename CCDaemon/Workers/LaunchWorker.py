import logging
from datetime import datetime

from CCDaemon.Workers import StatusWorker, PipelineRunner
from CCDaemon.Pipeline import PipelineStatus, PipelineError

class LaunchWorker(StatusWorker):
    # Main class for loading idle pipelines from database
    def __init__(self, db_helper, pipeline_queue, platform_factory, sleep_time=2):
        super(LaunchWorker, self).__init__(db_helper, pipeline_queue, sleep_time)

        # Factory for creating new pipeline runners
        self.platform_factory = platform_factory

    def task(self, session):

        # Get list of analysis pipelines that are ready to run
        idle_pipelines = self.db_helper.get_pipeline(session, status=PipelineStatus.IDLE)

        for pipeline in idle_pipelines:

            # Check to see if worker has been stopped externally
            if self.is_stopped():
                return

            # Check to see whether pipeline can be run
            if not self.__can_load_pipeline(pipeline):
                continue

            try:

                logging.info("Preparing to launch pipeline: '%s'!" % pipeline.name)

                # Get PipelineWorker for running pipeline
                platform            = self.platform_factory.get_platform(name=str(pipeline.analysis_id))
                config_file_strings = self.db_helper.get_config_file_strings(pipeline)
                pipeline_worker     = PipelineRunner(pipeline, config_file_strings, platform)

                # Set status in DB to loading and commit the latest change
                self.db_helper.update_status(pipeline, status=PipelineStatus.READY)

                # Begin running the pipeline
                pipeline_worker.start()

                # Set run start time variable in database
                pipeline.run_start = datetime.now()

                # Enqueue pipeline worker into pipeline queue
                self.pipeline_queue.add_pipeline(pipeline_worker)

            except BaseException, e:

                # Log errors
                logging.error("Unable to launch pipeline: '%s'!" % pipeline.name)
                if e.message != "":
                    logging.error("Received the following error: %s" % e.message)

                # Record pipeline failure in DB
                self.db_helper.update_status(pipeline, status=PipelineStatus.FAILED)

                # Specify pipeline failure due to init error
                self.db_helper.update_error_type(pipeline, error_type=PipelineError.INIT, extra_error_msg=e.message)

                # Raise offending error because this shouldn't be happening
                raise

            finally:
                # Commit any database changes for pipelines
                session.commit()

    def __can_load_pipeline(self, pipeline):
        # Return true if pipeline can be loaded, false otherwise

        # Determine if pipeline queue meet pipeline resource requirements
        cpus        = pipeline.analysis_type.cpus
        if not self.pipeline_queue.can_add_pipeline(req_cpus=cpus):
            logging.debug("Unable to run pipeline due to resource limit: %s" % pipeline.analysis_id)
            return False

        # Determine if pipeline is currently running
        if self.pipeline_queue.contains_pipeline(pipeline_id=pipeline.analysis_id):
            logging.debug("Unable to run pipeline due duplicate pipeline in queue: %s" % pipeline.analysis_id)
            return False

        return True



