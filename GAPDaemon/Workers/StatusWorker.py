import sys
import logging
import threading
import time
import abc
import Queue

class StatusWorker(threading.Thread):
    # Main class for pulling results of finished pipelines and updating their status in the database
    __metaclass__ = abc.ABCMeta

    def __init__(self, db_helper, pipeline_queue, sleep_time=2):
        super(StatusWorker, self).__init__()

        # Database helper used to establish new connections to the database
        self.db_helper = db_helper

        # Queue for holding currently running pipelines
        self.pipeline_queue = pipeline_queue

        # Run as a daemon so thread will quit upon error in main program
        self.daemon = True

        # Number of seconds to pause before doing it's task again
        self.sleep_time = sleep_time

        # Boolean for whether status worker is stopped
        self.stopped = False

        # Lock for whether busy
        self.busy_lock = threading.Lock()

        # Generating a queue for the exceptions that appear in the current thread
        self.exception_queue = Queue.Queue()

    @abc.abstractmethod
    def task(self, session):
        # Task to be performed while running
        pass

    def run(self):
        # Run indefinitely, performing some task every
        logging.debug("(%s) started working!" % self.__class__.__name__)
        while not self.is_stopped():

            try:
                # Create new database session and run some task
                with self.db_helper.session_context() as session:
                    self.task(session)

                # Sleep for a user-defined number of seconds
                time.sleep(self.sleep_time)

            except BaseException, e:
                logging.error("(%s) stopped working!" % self.__class__.__name__)
                if e.message != "":
                    logging.error("Received the following error message: %s" % e.message)

                # Put exception in exception queue
                self.exception_queue.put(sys.exc_info())

                # Stop task upon error
                self.stop()

        # Log that work has stopped
        logging.debug("(%s) has stopped working!" % self.__class__.__name__)

    def check(self):
        # Check to see if thread has stopped.
        # Raise offending error if it has.
        if self.is_stopped() and not self.exception_queue.empty():
            exc_info = self.exception_queue.get()
            raise exc_info[0], exc_info[1], exc_info[2]

    def stop(self):
        with self.busy_lock:
            self.stopped = True

    def is_stopped(self):
        with self.busy_lock:
            return self.stopped