import os
import logging
import abc
from collections import OrderedDict
import subprocess as sp
import threading

from Process import Process

class Processor(object):
    __metaclass__ = abc.ABCMeta
    def __init__(self, name, nr_cpus, mem, **kwargs):
        self.name       = name
        self.nr_cpus    = nr_cpus
        self.mem        = mem

        # Get name of directory where logs will be written
        self.log_dir    = kwargs.pop("log_dir", None)

        # Ordered dictionary of processing being run by processor
        self.processes  = OrderedDict()

        # Boolean for whether processor is stopped
        self.locked = False

    def create(self):
        pass

    def destroy(self):
        pass

    def run(self, job_name, cmd):

        # Throw error if attempting to run command on stopped processor
        if self.locked:
            logging.error("(%s) Attempt to run process '%s' on stopped processor!" % (self.name, job_name))
            raise RuntimeError("Attempt to run process of stopped processor!")

        # Checking if logging is required
        if "!LOG" in cmd:

            # Generate name of log file
            log_file = "%s.log" % job_name
            if self.log_dir is not None:
                log_file = os.path.join(self.log_dir, log_file)

            # Generating all the logging pipes
            log_cmd_null    = " >>/dev/null 2>&1 "
            log_cmd_stdout  = " >>%s " % log_file
            log_cmd_stderr  = " 2>>%s " % log_file
            log_cmd_all     = " >>%s 2>&1 " % log_file

            # Replacing the placeholders with the logging pipes
            cmd = cmd.replace("!LOG0!", log_cmd_null)
            cmd = cmd.replace("!LOG1!", log_cmd_stdout)
            cmd = cmd.replace("!LOG2!", log_cmd_stderr)
            cmd = cmd.replace("!LOG3!", log_cmd_all)

        # Save original command
        original_cmd = cmd

        # Make any modifications to the command to allow it to be run on a specific platform
        cmd = self.adapt_cmd(cmd)

        # Run command using subprocess popen and add Popen object to self.processes
        logging.info("(%s) Process '%s' started!" % (self.name, job_name))
        logging.debug("(%s) Process '%s' has the following command:\n    %s" % (self.name, job_name, original_cmd))

        # Generating process arguments
        kwargs = dict()

        # Process specific arguments
        kwargs["cmd"] = original_cmd

        # Popen specific arguments
        kwargs["shell"] = True
        kwargs["stdout"] = sp.PIPE
        kwargs["stderr"] = sp.PIPE
        kwargs["preexec_fn"] = os.setsid

        # Add process to list of processes
        self.processes[job_name] = Process(cmd, **kwargs)

    def wait(self):
        # Returns when all currently running processes have completed
        for proc_name, proc_obj in self.processes.iteritems():
            self.wait_process(proc_name)

    def lock(self):
        # Prevent any additional processes from being run
        with threading.Lock():
            self.locked = True

    def unlock(self):
        # Allow processes to run on processor
        with threading.Lock():
            self.locked = False

    def stop(self):
        # Lock so that no new processes can be run on processor
        self.lock()

        # Kill all currently executing processes on processor
        for proc_name, proc_obj in self.processes.iteritems():
            if not proc_obj.is_complete() and proc_name.lower() != "destroy":
                logging.debug("Killing process: %s" % proc_name)
                proc_obj.terminate()

    def set_log_dir(self, new_log_dir):
        self.log_dir = new_log_dir

    def get_name(self):
        return self.name

    @abc.abstractmethod
    def wait_process(self, proc_name):
        pass

    @abc.abstractmethod
    def adapt_cmd(self, cmd):
        pass


