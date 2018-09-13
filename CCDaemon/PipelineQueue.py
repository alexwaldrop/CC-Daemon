import threading
from datetime import datetime

from CCDaemon.Pipeline import PipelineStatus

class DuplicateKeyError(Exception):
    def __init__(self, *args, **kwargs):
        super(DuplicateKeyError, self).__init__(*args, **kwargs)

class ResourceError(Exception):
    def __init__(self, *args, **kwargs):
        super(ResourceError, self).__init__(*args, **kwargs)

class PipelineQueue:
    # Container Class for holding pipeline workers actively running on the system
    def __init__(self, max_cpus, max_loading):

        # Read resource capacity options from config
        self.max_cpus       = max_cpus
        assert isinstance(max_cpus, int) and max_cpus > 0, "PipelineQueue error: Max CPUs is not an integer >0!"

        # Maximum number of pipelines that can be loading at a given moment
        self.load_limit = max_loading
        assert isinstance(max_loading, int) and max_loading > 0, "PipelineQueue error: Max Loading is not an integer >0!"

        # Variables for holding current resource usage levels
        self.curr_cpus          = 0

        # Initialize empty dictionary to hold PipelineWorkers
        self.pipeline_workers       = dict()
        self.queue_lock   = threading.Lock()

    @property
    def __num_loading(self):
        num_loading = 0
        for pipeline_id, pipeline in self.pipeline_workers.iteritems():
            if pipeline.get_status() in [PipelineStatus.READY, PipelineStatus.LOADING]:
                num_loading += 1
        return num_loading

    def can_add_pipeline(self, req_cpus):
        # Determine if a pipeline can be enqueued based on its resource requirements
        with self.queue_lock:
            # Check not CPU overload
            cpu_overload    = self.curr_cpus + req_cpus > self.max_cpus
            # Check not too many pipelines currently loading
            loading_overload = 1 + self.__num_loading > self.load_limit
            return not cpu_overload and not loading_overload

    def add_pipeline(self, pipeline_worker):
        with self.queue_lock:

            # Raise except if pipeline_worker already exists in queue
            if pipeline_worker.get_id() in self.pipeline_workers:
                raise DuplicateKeyError("Duplicate pipelines with same ID (%s) in PipelineQueue!" % pipeline_worker.get_id())

            # Add pipeline to pipeline queue
            self.pipeline_workers[str(pipeline_worker.get_id())] = pipeline_worker

            # Increment resource levels
            self.curr_cpus += pipeline_worker.get_cpus()

            # Check resource limits and raise exception if any exceed maximum
            pipe_id = pipeline_worker.get_id()
            if self.curr_cpus > self.max_cpus:
                raise ResourceError("PipelineQueue cpu limit (%s) exceeded adding pipeline '%s'" %
                                    (self.max_cpus, pipe_id))

    def remove_pipeline(self, pipeline_id):
        # Remove one or more pipelines from the queue
        with self.queue_lock:

            # Get pipeline worker to remove
            pipeline_worker = self.pipeline_workers[str(pipeline_id)]

            # Remove pipeline from pipeline queue
            self.pipeline_workers.pop(str(pipeline_worker.get_id()))

            # Free up resources
            self.curr_cpus -= pipeline_worker.get_cpus()

    def get_pipeline(self, pipeline_id):
        with self.queue_lock:
            return self.pipeline_workers[str(pipeline_id)]

    def get_pipelines(self):
        with self.queue_lock:
            return self.pipeline_workers

    def contains_pipeline(self, pipeline_id):
        with self.queue_lock:
            return str(pipeline_id) in self.pipeline_workers.keys()

    def is_empty(self):
        with self.queue_lock:
            return len(self.pipeline_workers.keys()) == 0

    def __str__(self):
        # Print pipeline queue
        usage_stats = "Curr Usage: %s CPUs, %s Loading Pipelines" % (self.curr_cpus, self.__num_loading)
        max_usage_stats = "Max Usage: %s CPUs, %s Loading Pipelines" % (self.max_cpus, self.load_limit)

        with self.queue_lock:
            to_return = "Pipeline\tStatus\tRuntime\n"
            for pipeline in self.pipeline_workers.itervalues():
                # Print report for pipeline
                start_time = pipeline.get_start_time()
                runtime = 0 if start_time is None else self.__time_elapsed(start_time, datetime.now())
                to_return += "%s\t%s\t%f\n" % (pipeline.get_id(),
                                               pipeline.get_status(),
                                               runtime)
        # Surround by buffer string for aesthetics
        buffer_string = "*"*32
        to_return = "%s\n%s\n%s\n%s\n%s\n%s\n%s\n" % \
                    (buffer_string, usage_stats, max_usage_stats, buffer_string, to_return, buffer_string, buffer_string)
        return to_return

    def set_max_cpus(self, new_max_cpus):
        with self.queue_lock:
            self.max_cpus = new_max_cpus

    def set_max_loading(self, new_load_limit):
        with self.queue_lock:
            self.load_limit = new_load_limit

    @staticmethod
    def __time_elapsed(start, end):
        # Return the number of hours that have passed between two datetime intervals
        diff = end - start
        days, seconds = diff.days, diff.seconds
        hours = days * 24 + (seconds / 3600.0)
        return hours

