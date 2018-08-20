import logging
import base64
import json
import subprocess as sp

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
        cmd = "gcloud beta pubsub subscriptions pull --max-messages=1 --format=json %s" % subscription
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
                data = base64.b64decode(base64.b64decode(data))

        return msg_id, data

    @staticmethod
    def acknowledge_message(subscription, message_id):
        cmd = "gcloud beta pubsub subscriptions ack %s %s" % (subscription, message_id)
        err_msg = "Could not acknowledge a message from Google Pub/Sub"
        PubSub._run_cmd(cmd, err_msg=err_msg)

    @staticmethod
    def send_message(topic, message=None, attributes=None, encode=True):
        # Send a message to an existing Google cloud Pub/Sub topic

        # Return if message and attributes are both empty
        if message is None and attributes is None:
            return

        # Parse the input message and attributes
        message = "" if message is None else message
        attributes = {} if attributes is None else attributes

        # Encode the message if needed
        if encode:
            message = base64.b64encode(message)

        # Parse the attributes and pack into a single data structure message
        attrs = ",".join(["%s=%s" % (str(k), str(v)) for k, v in attributes.iteritems()])

        # Run command to publish message to the topic
        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub topics publish %s \"%s\" --attribute=%s" \
              % (topic, message, attrs)

        err_msg = "Could not send a message to Google Pub/Sub"
        PubSub._run_cmd(cmd, err_msg=err_msg)

    @staticmethod
    def subscription_exists(subscription):
        # Return True if pubsub subscription exists, false otherwise
        cmd = "gcloud beta pubsub subscriptions list --format=json --filter=\"subscriptionId=%s\"" % subscription
        err_msg = "Could not check Google Pub/Sub subscriptions!"
        out = PubSub._run_cmd(cmd, err_msg=err_msg)
        return (out.strip() != "[]")

    @staticmethod
    def topic_exists(topic):
        # Return True if pubsub topic exists, false otherwise
        cmd = "gcloud beta pubsub topics list --format=json --filter=\"topicId=%s\"" % topic
        err_msg = "Could not check Google Pub/Sub topic!"
        out = PubSub._run_cmd(cmd, err_msg=err_msg)
        return (out.strip() != "[]")

    @staticmethod
    def is_json(json_string):
        try:
            json.loads(json_string)
            return True
        except ValueError:
            return False

