import logging
import json

from PipelineOutputFile import PipelineOutputFile

class PipelineReport(object):

    def __init__(self, report_id, data):

        self.id = report_id
        self.data = data

        # Report components
        self.pipeline_id    = None
        self.error          = None
        self.is_success     = None
        self.files          = None
        self.cost           = None
        self.git_commit     = None

        # Boolean flag for whether report is valid
        self.valid_report = False

        # Try to parse report data
        self.parse_report()

    def is_valid(self):
        return self.valid_report

    def parse_report(self):
        # Attempt to set data members from report data
        try:
            # Parse data into json
            self.data = json.loads(self.data)

            # Get primary report components
            self.pipeline_id    = self.data["pipeline_id"]
            self.error          = self.data["error"]
            self.git_commit    = self.data["git_commit"]
            print self.error
            self.is_success     = self.data["status"] == "Complete"
            self.cost           = self.data["total_cost"]

            # Parse and create file objects from all output files declared in report
            files               = self.data["files"]
            self.files          = self.parse_files(files)

            # Indicate that report is valid
            self.valid_report   = True

        except BaseException, e:
            if self.pipeline_id is None:
                logging.warning("Invalid pipeline report!")
            else:
                logging.warning("Invalid pipeline report for pipeline '{0}'".format(self.pipeline_id))
            if e.message != "":
                logging.warning("Received the following message: %s" % e.message)
            # Indicate that report is invalid if unable to parse for any reason
            self.valid_report = False

    def parse_files(self, report_files):
        # Parse and create file objects from all output files declared in report
        parsed_files = []
        for report_file in report_files:
            file_type       = report_file["file_type"]
            path            = report_file["path"]
            is_final_output = report_file["is_final_output"]
            node_id         = report_file["task_id"]
            if is_final_output:
                parsed_files.append(PipelineOutputFile(path, filetype=file_type, node_id=node_id))
        return parsed_files

    def get_id(self):
        # Return report ID
        return self.id

    def get_pipeline_id(self):
        # Return pipeline id associated with report
        return self.pipeline_id

    def is_successful(self):
        # Return whether pipeline successfully completed
        return self.is_success

    def get_error_msg(self):
        # Return error messages if pipeline unsuccessful
        return self.error

    def get_files(self):
        # Return list of OutputFile objects declared by pipeline in the report
        return self.files

    def get_report_files(self):
        return [rf for rf in self.files if rf.get_filetype() == "qc_report"]

    def set_error_msg(self, err_msg):
        # Set an error message for the pipeline
        self.error = err_msg

    def set_successful(self, is_success):
        # Set whether or not pipeline was successful
        self.is_success = is_success

    def get_cost(self):
        return  self.cost

    def get_git_commit(self):
        return self.git_commit

    def __str__(self):
        to_ret = "********* Pipeline Report: ***********\n"
        to_ret += "ID: %s\n" % self.id
        to_ret += "Pipeline: %s\n" % self.pipeline_id
        to_ret += "Success: %s\n" % self.is_success
        to_ret += "Error msg: %s\n" % self.error
        to_ret += "Out files:"
        for out_file in self.files:
            to_ret += "\n%s" % out_file
        return to_ret

