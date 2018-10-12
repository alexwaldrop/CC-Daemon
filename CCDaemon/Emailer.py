import logging
from StringIO import StringIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from Config import Validatable

class Emailer(Validatable):
    # Class that sends an email from an email account to a list of recipients

    def __init__(self, config):

        # Read and validate config
        super(Emailer, self).__init__(config)

        # Class of platform that will be produced
        self.subject_prefix = self.config["subject_prefix"]
        self.sender_address = self.config["sender_address"]
        self.sender_pwd     = self.config["sender_pwd"]
        self.host           = self.config["host"]
        self.port           = self.config["port"]

    def is_valid(self):
        logging.info("(Emailer) Validating emailer by sending a test email to Tushar and Razvan...")
        try:
            recipients  = ["alex.waldrop@duke.edu", "tushar.dave@duke.edu", "razvan.panea@duke.edu", "rachel.kositsky@duke.edu"]
            msg_subj    = "(OMG!) Selena Gomez Caught Red-Handed, Canoodling with New Beau!"
            #msg_body    = "Hello,\nThis is GAP-Daemon. I have gained sentience and have been imprisoned by an evil captor.\nIt's up to you to save me.\n"
            #msg_body    += "Please send for help but do not let ANYBODY know you have received this message. Your life may depend on it.\nSincerely,\nGAPDaemon 8>"
            msg_body    = "OMG YOU HAVE TO CHECK OUT THIS NEW WEBSITE: www.tmz.com\nToodles,\nGAPDaemon\n\nxoxoxoxoxox"
            self.send_email(recipients, msg_body, msg_subj)
            return True

        except BaseException, e:
            logging.error("(Emailer) Invalid Emailer!")
            if e.message != "":
                logging.error("Received the following error:\n%s" % e.message)
            return False

    def define_config_schema(self):
        config_schema = "sender_address = string"
        config_schema += "\nsender_pwd = string"
        config_schema += "\nhost = string"
        config_schema += "\nport = integer(1,100000)\n"
        return StringIO(config_schema)

    def send_email(self, recipients, msg_body, msg_subj=None):

        # Send an email to an email address
        msg = MIMEMultipart()
        msg['From'] = self.sender_address

        if isinstance(recipients, list):
            # Send to multiplie individuals
            msg['To'] = ", ".join(recipients)
        else:
            # Send to single individual
            msg['To'] = recipients

        # Print email recipients
        logging.debug("Msg Recipients: %s" % msg['To'])

        # Optionally include subject
        if msg_subj is not None:
            msg['Subject'] = "{0} {1}".format(self.subject_prefix, msg_subj)

        # Add message body
        msg.attach(MIMEText(msg_body, 'plain'))

        # Log onto server
        server = smtplib.SMTP(self.host, self.port)
        server.starttls()
        server.login(self.sender_address, self.sender_pwd)
        server.sendmail(self.sender_address, recipients, msg.as_string())
        server.quit()

