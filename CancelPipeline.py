#!/usr/bin/env python2.7
import argparse
import logging
import os
import sys

from Config import ConfigParser
from CCDaemon.Database import DBHelper
from CCDaemon.Pipeline import PipelineStatus, PipelineError
from RunDaemon import configure_logging


def configure_argparser(argparser_obj):

    def file_type(arg_string):
        """
        This function check both the existance of input file and the file size
        :param arg_string: file name as string
        :return: file name as string
        """
        if not os.path.exists(arg_string):
            err_msg = "%s does not exist!! " \
                      "Please provide a correct file!!" % arg_string
            raise argparse.ArgumentTypeError(err_msg)

        return arg_string

    # Path to CC Daemon config file
    argparser_obj.add_argument("--config",
                               action="store",
                               type=file_type,
                               dest="config_file",
                               required=True,
                               help="Path to config file containing input files "
                                    "and information for one or more samples.")

    # Type of action to be performed on pipeline queue
    argparser_obj.add_argument("--pipeline-id",
                               action="store",
                               type=int,
                               dest="pipeline_id",
                               required=True,
                               help="Database ID of pipeline to cancel.")

def main():

    # Boolean for whether pipeline was successfully cancelled in database
    cancelled = False

    # Configure argparser
    argparser = argparse.ArgumentParser(prog="CC-Daemon-Cancel")
    configure_argparser(argparser)

    # Parse the arguments
    args = argparser.parse_args()

    # Configure logging
    configure_logging(1)

    # Read config file
    exec_dir = sys.path[0]
    config_schema = os.path.join(exec_dir, "CCDaemon/CCDaemon.validate")
    config = ConfigParser(args.config_file, config_spec=config_schema).get_config()

    try:
        # Connect to database
        logging.info("Connecting to database...")
        db_helper = DBHelper(username=config["db_helper"]["username"],
                             password=config["db_helper"]["password"],
                             database=config["db_helper"]["database"],
                             host=config["db_helper"]["host"],
                             mysql_driver=config["db_helper"]["mysql_driver"])

        # Create a session for interacting with database
        with db_helper.session_context() as session:

            # Check to see if pipeline id actually exists
            if not db_helper.pipeline_exists(session, args.pipeline_id):
                logging.error("Pipeline with id '%s' doesn't exist in database!" % args.pipeline_id)
                raise IOError("Invalid pipeline id: '%s'" % args.pipeline_id)

            # Get pipeline record and current status using ID
            pipeline_record = db_helper.get_pipeline(session, pipeline_id=args.pipeline_id)
            curr_status     = pipeline_record.status.description.upper()

            # Change pipeline status to CANCELLING if status permits
            active_statuses = [PipelineStatus.IDLE, PipelineStatus.READY, PipelineStatus.LOADING, PipelineStatus.RUNNING]
            if curr_status in active_statuses:
                db_helper.update_status(pipeline_record, status=PipelineStatus.CANCELLING)
                db_helper.update_error_type(pipeline_record, error_type=PipelineError.CANCEL, extra_error_msg="Manually cancelled by user.")
                cancelled = True
            else:
                logging.warning("Not cancelling because pipeline is past point of cancelling! Current status: '%s'" % curr_status)

    except BaseException, e:
        logging.error("CC-Daemon-Cancel failed! No changes were made to the database!")

    finally:
        # Report that Pipeline has successfully been cancelled
        if cancelled:
            logging.info("Successfully cancelled pipeline with id: %s!" % args.pipeline_id)
            exit(0)
        else:
            exit(1)

if __name__ == "__main__":
    main()
