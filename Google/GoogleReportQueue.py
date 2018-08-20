import logging

from PubSub import PubSub
from CCDaemon import ReportQueue

class GoogleReportQueue(ReportQueue):
    # Class for pulling results of finished pipelines from Google PubSub
    def __init__(self, config):
        super(GoogleReportQueue, self).__init__(config)

        # Google PubSub topic and subscription where reports are streamed
        self.report_sub      = self.config["report_sub"]
        self.report_topic    = self.config["report_topic"]

    def pull_report(self):
        return PubSub.get_message(self.report_sub)

    def pop_report(self, report_id):
        PubSub.acknowledge_message(self.report_sub, report_id)

    def is_valid(self):
        # Return True if PubSub subscription and topics exist, false otherwise
        sub_exists = PubSub.subscription_exists(self.report_sub)
        topic_exists = PubSub.topic_exists(self.report_topic)

        # Log appropriate error messages
        if not sub_exists:
            logging.error("(ReportQueue) Invalid report queue! PubSub subscription '%s' does not exist!" % self.report_sub)
        if not topic_exists:
            logging.error("(ReportQueue) Invalid report queue! PubSub topic '%s' does not exist!" % self.report_sub)

        return (sub_exists and topic_exists)