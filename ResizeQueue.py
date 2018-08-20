#!/usr/bin/env python2.7
import argparse
import logging
import os
import sys

from Config import ConfigParser
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

    # Path to GAP Daemon config file
    argparser_obj.add_argument("--config",
                               action="store",
                               type=file_type,
                               dest="config_file",
                               required=True,
                               help="Path to config file containing input files "
                                    "and information for one or more samples.")

    # Type of action to be performed on pipeline queue
    argparser_obj.add_argument("--action",
                               action="store",
                               type=str,
                               dest="action",
                               required=True,
                               choices=["INCREASE", "DECREASE", "LOCK", "RESET", "MANUAL"],
                               help="Action to perform on pipeline queue. Options: INCREASE, DECREASE, LOCK, RESET, MANUAL")

    # Type of action to be performed on pipeline queue
    argparser_obj.add_argument("--maxcpus",
                               action="store",
                               type=int,
                               dest="max_cpus",
                               required=False,
                               default=-1,
                               help="Maximum cpu limit to use if action is 'MANUAL'")

    # Type of action to be performed on pipeline queue
    argparser_obj.add_argument("--maxmem",
                               action="store",
                               type=int,
                               dest="max_mem",
                               required=False,
                               default=-1,
                               help="Maximum mem limit to use if action is 'MANUAL'")

    # Type of action to be performed on pipeline queue
    argparser_obj.add_argument("--maxdiskspace",
                               action="store",
                               type=int,
                               dest="max_disk_space",
                               required=False,
                               default=-1,
                               help="Maximum disk space limit to use if action is 'MANUAL'")

def main():

    # Configure argparser
    argparser = argparse.ArgumentParser(prog="GAP-Daemon-Resize")
    configure_argparser(argparser)

    # Parse the arguments
    args = argparser.parse_args()

    # Configure logging
    configure_logging(3)

    try:

        # Read config file
        exec_dir = sys.path[0]
        config_schema = os.path.join(exec_dir, "GAPDaemon/GAPDaemon.validate")
        config = ConfigParser(args.config_file, config_spec=config_schema).get_config()

        # Get current resource limits
        max_cpus        = config["pipeline_queue"]["max_cpus"]
        max_mem         = config["pipeline_queue"]["max_mem"]
        max_disk_space  = config["pipeline_queue"]["max_disk_space"]

        # Change resource limit values as they appear in config
        if args.action == "INCREASE":
            # Double all if action is to increase pipeline queue size
            config["pipeline_queue"]["max_cpus"]        = int(max_cpus * 2)
            config["pipeline_queue"]["max_mem"]         = int(max_mem * 2)
            config["pipeline_queue"]["max_disk_space"]  = int(max_disk_space * 2)

        elif args.action == "DECREASE":
            # Halve if action is to decrease pipeline queue size
            config["pipeline_queue"]["max_cpus"]        = int(max_cpus / 2.0)
            config["pipeline_queue"]["max_mem"]         = int(max_mem / 2.0)
            config["pipeline_queue"]["max_disk_space"]  = int(max_disk_space / 2.0)

        elif args.action == "LOCK":
            # Set all to 0 if action is to lock pipeline queue
            config["pipeline_queue"]["max_cpus"]        = 0
            config["pipeline_queue"]["max_mem"]         = 0
            config["pipeline_queue"]["max_disk_space"]  = 0

        elif args.action == "RESET":
            # Reset pipeline queue to a new set of values
            config["pipeline_queue"]["max_cpus"]        = 4
            config["pipeline_queue"]["max_mem"]         = 16
            config["pipeline_queue"]["max_disk_space"]  = 50

        elif args.action == "MANUAL":
            # Manually set resource limits from command line input
            if args.max_cpus >= 0:
                config["pipeline_queue"]["max_cpus"]    = args.max_cpus

            if args.max_mem >= 0:
                config["pipeline_queue"]["max_mem"]     = args.max_mem

            if args.max_disk_space >= 0:
                config["pipeline_queue"]["max_disk_space"]  = args.max_disk_space

        # Overwrite original config file
        config.write()

        # Report that ResizeQueue finished successfully
        logging.info("Successfully updated pipeline queue!")
        logging.info("Current pipeline queue quotas:\nMax CPUs: %s\nMax mem (GB): %s\nMax disk space (GB): %s" %
                     (config["pipeline_queue"]["max_cpus"],
                      config["pipeline_queue"]["max_mem"],
                      config["pipeline_queue"]["max_disk_space"]))

    except BaseException, e:
        # Report any errors that arise
        logging.error("ResizeQueue failed!")
        if e.message != "":
            logging.error("Received the following error message:\n%s" % e.message)
        raise

if __name__ == "__main__":
    main()
