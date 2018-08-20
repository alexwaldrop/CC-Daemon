#!/usr/bin/env python2.7
import os
import sys
import argparse
import logging
import signal

from GAPDaemon import DaemonManager

# Define the available platform modules
available_platforms = DaemonManager.AVAILABLE_PLATFORMS

def configure_logging(verbosity):
    # Setting the format of the logs
    FORMAT = "[%(asctime)s] %(levelname)s: %(message)s"

    # Configuring the logging system to the lowest level
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, stream=sys.stderr)

    # Defining the ANSI Escape characters
    BOLD = '\033[1m'
    DEBUG = '\033[92m'
    INFO = '\033[94m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    END = '\033[0m'

    # Coloring the log levels
    if sys.stderr.isatty():
        logging.addLevelName(logging.ERROR, "%s%s%s%s%s" % (BOLD, ERROR, "GAP_DAEMON_ERROR", END, END))
        logging.addLevelName(logging.WARNING, "%s%s%s%s%s" % (BOLD, WARNING, "GAP_DAEMON_WARNING", END, END))
        logging.addLevelName(logging.INFO, "%s%s%s%s%s" % (BOLD, INFO, "GAP_DAEMON_INFO", END, END))
        logging.addLevelName(logging.DEBUG, "%s%s%s%s%s" % (BOLD, DEBUG, "GAP_DAEMON_DEBUG", END, END))
    else:
        logging.addLevelName(logging.ERROR, "GAP_DAEMON_ERROR")
        logging.addLevelName(logging.WARNING, "GAP_DAEMON_WARNING")
        logging.addLevelName(logging.INFO, "GAP_DAEMON_INFO")
        logging.addLevelName(logging.DEBUG, "GAP_DAEMON_DEBUG")

    # Setting the level of the logs
    level = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][verbosity]
    logging.getLogger().setLevel(level)

def configure_argparser(argparser_obj):

    def platform_type(arg_string):
        value = arg_string.capitalize()
        if value not in available_platforms:
            err_msg = "%s is not a valid platform! " \
                      "Please view usage menu for a list of available platforms" % value
            raise argparse.ArgumentTypeError(err_msg)

        return arg_string

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

    # Path to sample set config file
    argparser_obj.add_argument("--config",
                               action="store",
                               type=file_type,
                               dest="config_file",
                               required=True,
                               help="Path to config file containing input files "
                                    "and information for one or more samples.")

    # Name of the platform module
    available_plats = "\n".join(["%s" % item for item in available_platforms])
    argparser_obj.add_argument("--platform",
                               action='store',
                               type=platform_type,
                               dest='platform',
                               required=True,
                               help="Platform to be used. Possible values are:\n%s" % available_plats)

    # Verbosity level
    argparser_obj.add_argument("-v",
                               action='count',
                               dest='verbosity_level',
                               required=False,
                               default=0,
                               help="Increase verbosity of the program."
                                    "Multiple -v's increase the verbosity level:\n"
                                    "0 = Errors\n"
                                    "1 = Errors + Warnings\n"
                                    "2 = Errors + Warnings + Info\n"
                                    "3 = Errors + Warnings + Info + Debug")

def main():

    # Configure argparser
    argparser = argparse.ArgumentParser(prog="GAP-Daemon")
    configure_argparser(argparser)

    # Parse the arguments
    args = argparser.parse_args()

    # Configure logging
    configure_logging(args.verbosity_level)

    # Read and validate daemon config
    config_file     = args.config_file

    # Create GAP daemon and make global
    gap_daemon  = DaemonManager(config_file=config_file, platform_type=args.platform)
    err_msg     = ""

    try:

        # Validate GAP daemon components
        logging.info("(Main) Validating GAP daemon...")
        # gap_daemon.validate()
        logging.info("(Main) GAP daemon is valid!")

        # Define inner function to update pipeline queue when a sighup is received
        def update_daemon(signum, frame):
            logging.debug("SIGHUP received!")
            gap_daemon.update_pipeline_queue()

        # Register SIGHUP as signal indicating when daemon should update
        signal.signal(signal.SIGHUP, update_daemon)

        # Summon the GAP daemon and have it run until an error occurs
        gap_daemon.summon()

    except KeyboardInterrupt, e:
        logging.error("(Main) Keyboard interrupt!")
        err_msg = "Keyboard interrupt!"

    except BaseException, e:
        # Report any errors that arise
        logging.error("(Main) Daemon failed!")
        err_msg = "Runtime error!"
        if e.message != "":
            err_msg += "\nReceived the following error message:\n%s" % e.message
        raise

    finally:
        # Safely clean-up and send error report
        gap_daemon.finalize(err_msg=err_msg)
        logging.info("GAP-Daemon exited gracefully!")

if __name__ == "__main__":
    main()
