import abc
import json
import logging

from Config import Validatable
from CCDaemon.Pipeline import PipelineReport


class ReportQueue(Validatable):
    __metaclass__ = abc.ABCMeta
    # Abstract Base class for pulling results of finished pipelines from some data source

    def __init__(self, config):
        super(ReportQueue, self).__init__(config)

    def pull(self):

        try:
            # Return a report object from the report queue
            report_id, report_data = self.pull_report()
            if report_id is not None:
                report_data = json.loads(report_data)
                return PipelineReport(report_id=report_id, data=report_data)

            # Return none if report has no ID as nothing can be done with the report
            return None

        except BaseException, e:
            logging.error("Error trying to pull from report queue!")
            if e.message != "":
                logging.error("The following error was received: %s" % e.message)
            raise

    def pop(self, report):

        try:
            # Remove a report object from the report queue
            report_id = report.get_id()
            self.pop_report(report_id)

        except BaseException, e:
            logging.error("Could not pop a report from the report queue!")
            if e.message != "":
                logging.error("The following error was received: %s" % e.message)
            raise

    def is_valid(self):
        return True

    def define_config_schema(self):
        return None

    @abc.abstractmethod
    def pull_report(self):
        # Abstract method pulls and returns a pipeline report from data source where pipeline results are streamed
        # Returns None if not reports available
        pass

    @abc.abstractmethod
    def pop_report(self, report_id):
        pass





