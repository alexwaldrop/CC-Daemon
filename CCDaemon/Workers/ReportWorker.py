import logging
import abc

from CCDaemon.Workers import StatusWorker
from CCDaemon.Pipeline import PipelineStatus, PipelineError, QCReport, parse_qc_report

class ReportWorker(StatusWorker):
    # Main class for pulling results of finished pipelines and updating their status in the database
    __metaclass__ = abc.ABCMeta

    def __init__(self, db_helper, pipeline_queue, report_queue, platform, sleep_time=2):
        super(ReportWorker, self).__init__(db_helper, pipeline_queue, sleep_time)

        # Initialize results report queue
        self.report_queue = report_queue

        # Set platform
        self.platform = platform

    def task(self, session):
        # Get a report from the pipeline queue and attempt to record its information in the database

        # Pull report from database
        report = self.report_queue.pull()

        if report is None:
            # Return if not report
            return

        if report.is_valid():
            # Process report if it's valid and is contained within pipeline launcher
            logging.debug("(ReportWorker) Received pipeline report:\n%s\n" % report)

            # Don't process any reports currently on pipeline queue
            if self.pipeline_queue.contains_pipeline(pipeline_id=report.get_pipeline_id()):
                logging.debug("(ReportWorker) Not adding pipeline report to database because pipeline still present in pipeline queue!")
                return

            # Check to see if pipeline is actually in database
            if not self.db_helper.pipeline_exists(session, pipeline_id=report.get_pipeline_id()):
                logging.debug("(ReportWorker) Not adding pipeline report to database because pipeline id doesn't appear in database")
                # Remove report from report queue because it will never actually be processed
                self.report_queue.pop(report)
                return

            # Check to see if pipeline has already been reported
            pipeline = self.db_helper.get_pipeline(session=session, pipeline_id=report.get_pipeline_id())
            if pipeline.cost is not None:
                # Remove report from queue because you don't want to keep re-processing reports
                self.report_queue.pop(report)
                return

            # See if any files declared in report are missing
            logging.debug("(ReportWorker) Checking output files...")
            self.check_output_files(report)

            # Update report based on whether any output files were missing
            logging.debug("(ReportWorker) Updating report...")
            report = self.update_report(report)

            # Post report in database
            logging.debug("(ReportWorker) Updating database...")
            self.update_database(session, report)

        # Remove the report from the queue
        self.report_queue.pop(report)

    def update_database(self, session, report):
        # Update pipeline results in the database to reflect information contained in the report

        # Get pipeline record from database
        pipeline = self.db_helper.get_pipeline(session=session, pipeline_id=report.get_pipeline_id())

        # Update pipeline cost
        pipeline.cost = report.get_cost()

        # Update CC git version
        git_commit = report.get_git_commit()
        if git_commit is not None:
            pipeline.git_commit = git_commit

        # Add output file information regardless of whether pipeline was successful
        qc_entries_seen = []
        for report_file in report.get_files():
            if report_file.is_found():
                self.db_helper.register_output_file(pipeline, report_file)

                # Parse file if qc_report
                if report_file.get_filetype() == "qc_report":

                    try:
                        # Try to Parse QCFile
                        qc_stats = self.parse_qc_report(report_file.get_path())

                        # Add stats parsed from qc file if not already there
                        for qc_stat in qc_stats:
                            stat_id = "{0}_{1}_{2}_{3}".format(qc_stat.get_sample_id(),
                                                                   qc_stat.get_key(),
                                                                   qc_stat.get_task_id(),
                                                                   qc_stat.get_input_file())

                            # Add to database if not already seen
                            if stat_id not in qc_entries_seen:
                                self.db_helper.register_qc_stat(pipeline, qc_stat)
                                qc_entries_seen.append(stat_id)

                    except BaseException, e:
                        logging.warning("Unable to add qc stats to database for file: {0}".format(report_file.get_path()))
                        if e.message != "":
                            logging.warning("Received the following error:\n{0}".format(e.message))


        # Update pipeline status and error type
        if report.is_successful():
            logging.debug("(ReportWorker) Writing results for successful pipeline to database!")
            # Always update database for successful pipelines
            self.db_helper.update_status(pipeline, status=PipelineStatus.SUCCESS)
            self.db_helper.update_error_type(pipeline, error_type=PipelineError.NONE)

        elif pipeline.error is None:
            logging.debug("(ReportWorker) Writing results for failed pipeline because none exist in db for %s!" % pipeline.analysis_id)
            # Update database for failed pipelines if there isn't anything currently in the database
            self.db_helper.update_status(pipeline, status=PipelineStatus.FAILED)
            self.db_helper.update_error_type(pipeline, error_type=PipelineError.RUN,
                                             extra_error_msg=report.get_error_msg())

        elif pipeline.error.error_type in [PipelineError.REPORT, PipelineError.RUN]:
            logging.debug("(ReportWorker) Overwriting dummy report in database for pipeline: %s" % pipeline.analysis_id)
            # Update database for failed pipelines if current db status is Report fail or Run fail
            self.db_helper.update_status(pipeline, status=PipelineStatus.FAILED)
            self.db_helper.update_error_type(pipeline, error_type=PipelineError.RUN,
                                             extra_error_msg=report.get_error_msg())

        # Commit any changes made during the session
        session.commit()

    def check_output_files(self, report):
        # Check whether declared output files actually exist on platform
        for report_file in report.get_files():
            if self.platform.path_exists(report_file.get_path()):
                logging.debug("(ReportWorker) File exists: %s" % report_file)
                report_file.mark_as_found()

    @staticmethod
    def update_report(report):
        # Modify report to include information about missing output files

        missing_files = False
        missing_file_error_msg = "One or more output files declared in report doesn't exist! " \
                                 "The following could not be located: "

        # Add error message for any missing files declared in the report
        for output_file in report.get_files():
            # Add missing file to error msg
            if not output_file.is_found():
                logging.debug("Missing file: %s" % output_file)
                missing_files = True
                missing_file_error_msg += "\n%s" % output_file

        # Create final error message if any files were missing
        if missing_files:
            # Indicate that pipeine was not successful as one or more files declared don't actually exist
            report.set_successful(is_success=False)
            final_err_msg = report.get_error_msg()
            if final_err_msg != "":
                final_err_msg += "\n\n***** Additional Error *****\n%s" % missing_file_error_msg
            else:
                final_err_msg = missing_file_error_msg

            # Update report error message
            report.set_error_msg(final_err_msg)

        # Return updated report
        return report

    def parse_qc_report(self, qc_report_file):
        # Return list of QCStat entries

        # Read file contents from stdout
        report_data = self.platform.cat_file(qc_report_file)

        # Read file into qc_report
        qc_report = parse_qc_report(report_data)

        # Don't unpack if multi-sample qc-report
        #if len(qc_report.get_sample_names()) > 1:
        #    return []

        # Unpack qc_stats into list
        qc_stats = []
        for sample in qc_report.get_sample_names():
            sample_data = qc_report.get_sample_data(sample)
            for qc_entry in sample_data:
                qc_stats.append(QCStat(sample, qc_entry))

        return qc_stats

class QCStat:
    def __init__(self, sample_name, sample_data):
        self.sample_name    = sample_name
        self.key            = sample_data["Name"]
        self.value          = sample_data["Value"]
        self.task_id        = sample_data["Module"]
        self.input_file     = sample_data["Source"]
        self.notes          = sample_data["Note"]

    def get_key(self):
        return  self.key

    def get_sample_id(self):
        return self.sample_name

    def get_value(self):
        return self.value

    def get_task_id(self):
        return self.task_id

    def get_input_file(self):
        return self.input_file

    def get_notes(self):
        return self.notes
