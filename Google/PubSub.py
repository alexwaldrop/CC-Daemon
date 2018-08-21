import logging
import base64
import json
import subprocess as sp
import zlib

class PubSub(object):

    @staticmethod
    def _run_cmd(cmd, err_msg=None):

        # Running and waiting for the command
        proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out, err = proc.communicate()

        # Check if any error has appeared
        if len(err) != 0 and "error" in err.lower():
            logging.error("Google Pub/Sub stopped working!")
            if err_msg is not None:
                logging.error("%s. The following error appeared:\n    %s" % (err_msg, err))
            raise RuntimeError("Google Pub/Sub stopped working!")

        return out

    @staticmethod
    def get_message(subscription):
        # Function pops next message from a PubSub subscription
        # Decodes message and returns message contents
        cmd = "gcloud pubsub subscriptions pull --format=json %s" % subscription
        err_msg = "Could not receive a message from Google Pub/Sub"
        out = PubSub._run_cmd(cmd, err_msg=err_msg)

        # Parsing the output
        msg_id      = None
        data        = None
        msg_json    = []

        # Check to see if output returned is a valid json string
        if PubSub.is_json(out):
            msg_json = json.loads(out)

        if len(msg_json) != 0:

            msg_id  = msg_json[0]["ackId"]
            msg     = msg_json[0]["message"]

            # Obtain the information
            data = msg.get("data", None)

            # Decode the data
            if data is not None:
                data = zlib.decompress(base64.b64decode(base64.b64decode(data)))

        return msg_id, data

    @staticmethod
    def acknowledge_message(subscription, message_id):
        cmd = "gcloud pubsub subscriptions ack %s --ack-ids=%s" % (subscription, message_id)
        err_msg = "Could not acknowledge a message from Google Pub/Sub"
        PubSub._run_cmd(cmd, err_msg=err_msg)

    @staticmethod
    def subscription_exists(subscription):
        # Return True if pubsub subscription exists, false otherwise
        cmd = "gcloud pubsub subscriptions list --format=json"
        err_msg = "Could not check Google Pub/Sub subscriptions!"
        out = PubSub._run_cmd(cmd, err_msg=err_msg)

        subs = json.loads(out)
        for sub in subs:
            if sub["name"].split("/")[-1] == subscription:
                return True
        return False

    @staticmethod
    def topic_exists(topic_id):
        # Return True if pubsub topic exists, false otherwise
        cmd = "gcloud pubsub topics list --format=json"
        err_msg = "Could not check Google Pub/Sub topic!"
        out = PubSub._run_cmd(cmd, err_msg=err_msg)

        topics = json.loads(out)
        for topic in topics:
            if topic["name"].split("/")[-1] == topic_id:
                return True
        return False

    @staticmethod
    def is_json(json_string):
        try:
            json.loads(json_string)
            return True
        except ValueError:
            return False

