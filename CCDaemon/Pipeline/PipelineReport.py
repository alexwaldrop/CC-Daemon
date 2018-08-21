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
            self.pipeline_id    = self.data["pipeline_ID"]
            self.error          = self.data["error"]
            print self.error
            self.is_success     = self.data["status"] == "Complete"

            # Parse and create file objects from all output files declared in report
            files               = self.data["files"]
            self.files          = self.parse_files(files)

            # Indicate that report is valid
            self.valid_report   = True

        except BaseException, e:
            logging.error("PipelineReport Error: Unable to parse report!")
            if e.message != "":
                logging.error("Received the following message: %s" % e.message)
            # Indicate that report is invalid if unable to parse for any reason
            self.valid_report = False

    def parse_files(self, report_files):
        # Parse and create file objects from all output files declared in report
        parsed_files = []
        for node_id, node_files in report_files.iteritems():

            for file_type, file_paths in node_files.iteritems():
                if isinstance(file_paths, list):
                    # Case: Multiple files with same node_id, path_key
                    for file_path in file_paths:
                        report_file = PipelineOutputFile(file_path, filetype=file_type, node_id=node_id)
                        parsed_files.append(report_file)
                elif isinstance(file_paths, basestring):
                    # Case: Single file with node_id, path_key
                    report_file = PipelineOutputFile(file_paths, filetype=file_type, node_id=node_id)
                    parsed_files.append(report_file)

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

    def set_error_msg(self, err_msg):
        # Set an error message for the pipeline
        self.error = err_msg

    def set_successful(self, is_success):
        # Set whether or not pipeline was successful
        self.is_success = is_success

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

