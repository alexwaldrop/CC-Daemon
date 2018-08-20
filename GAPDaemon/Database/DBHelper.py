import base64
import logging
from contextlib import contextmanager

# SQLAlchemy imports
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

# Database related classes
from DatabaseModel import Analysis
from DatabaseModel import AnalysisError
from DatabaseModel import AnalysisOutput
from DatabaseModel import AnalysisStatus
from GAPDaemon.Database.DBError import DBError

# Pipeline Error and Status classes
from GAPDaemon.Pipeline import PipelineError
from GAPDaemon.Pipeline import PipelineStatus

class DBHelper(object):

    def __init__(self, username, password, database, host, mysql_driver):
        # Create URL object to connect to database
        self.url = URL(mysql_driver, username=username, password=password, database=database, host=host)

        # Generate a session maker
        self.session_factory = sessionmaker()

        # Establish a connection
        self.db_con = self.connect()

        # Sync statuses ids
        self.statuses = {}
        self.sync_statuses()

        # Synch error types
        self.error_types = {}
        self.sync_error_types()

    def connect(self, echo=False):

        # Create an engine
        try:
            logging.info("(DBHelper) Creating database connection engine to connect to database!" )
            engine = create_engine(self.url, echo=echo)

        except BaseException, e:
            logging.error("Unable to connect to database!")
            if e.message != "":
                logging.error("Received the following error message: %s" % e.message)

            raise DBError("Unable to connect to the database!")

        # Bind the engine to the session factory
        try:
            logging.info("(DBHelper) Creating the session factory for the database connection!")
            self.session_factory.configure(bind=engine)

        except BaseException, e:
            logging.error("Unable to create the session factory!")
            if e.message != "":
                logging.error("Received the following error message: %s" % e.message)

            raise DBError("Unable to create the session factory!")

        # Return the database connection
        logging.info("(DBHelper) Successfully connect to database!")
        return engine

    def disconnect(self):

        self.db_con.dispose()

    def get_pipeline(self, session, pipeline_id=None, status=None):

        # If ID provided, return the specific pipeline
        try:
            if pipeline_id is not None:
                return session.query(Analysis).\
                            filter(Analysis.analysis_id == pipeline_id).\
                            one()
        except NoResultFound:
            # Raise DB error if no pipelines found with ID
            raise DBError("No pipelines found with id: '%s'" % pipeline_id)

        # If status provided, return the pipelines with the specific status
        if status is not None:
            return session.query(Analysis).\
                            filter(Analysis.status_id == self.statuses[status]).\
                            all()

        # No filtering provided, so return all the pipelines
        return session.query(Analysis).\
                        all()

    def sync_statuses(self):

        with self.session_context() as session:

            for status in PipelineStatus.status_list:
                # Add analysis status if not present in DB
                if len(session.query(AnalysisStatus).filter(AnalysisStatus.description == status.lower()).all()) == 0:
                    session.add(AnalysisStatus(description=status.lower()))
                    session.flush()

                # Map PipelineStatus to it's DB id
                try:
                    self.statuses[status] = session.query(AnalysisStatus).\
                                            filter(AnalysisStatus.description == status.lower()).\
                                            one().\
                                            status_id

                except MultipleResultsFound:
                    logging.error("There are multiple records in the database that have the status: %s. "
                                  "Please ensure only one status_id is given to the same status label" % status)

                    raise DBError("Could not load status id for '%s'. "
                                  "Please ensure that the status is defined in the DB." % status)

                except NoResultFound:
                    logging.error("There is no status with name '%s' in the database. Please define one." % status)

                    raise DBError("There is no status with name '%s' defined in the DB" % status)

    def sync_error_types(self):

        with self.session_context() as session:

            for error_type in PipelineError.error_types:
                # Add analysis status if not present in
                if len(session.query(AnalysisError).filter(AnalysisError.error_type == error_type.lower()).all()) == 0:
                    # Get error message associated with error type
                    err_msg = PipelineError.error_msgs[error_type]
                    session.add(AnalysisError(description=err_msg, error_type=error_type.lower()))
                    session.flush()

                # Map PipelineStatus to it's DB id
                try:
                    self.error_types[error_type] = session.query(AnalysisError).\
                                            filter(AnalysisError.error_type == error_type.lower()).\
                                            one().\
                                            error_id

                except MultipleResultsFound:
                    logging.error("There are multiple records in the database that have the status: %s. "
                                  "Please ensure only one status_id is given to the same status label" % error_type)

                    raise DBError("Could not load status id for '%s'. "
                                  "Please ensure that the status is defined in the DB." % error_type)

                except NoResultFound:
                    logging.error("There is no status with name '%s' in the database. Please define one." % error_type)

                    raise DBError("There is no status with name '%s' defined in the DB" % error_type)

    def update_status(self, pipeline, status):

        # Do not set the
        if status not in self.statuses:
            raise DBError("No status with name %s is defined in the database!" % status)

        # Update the status of the pipeline
        pipeline.status_id = self.statuses[status]

    def update_error_type(self, pipeline, error_type, extra_error_msg=""):

        # Do not set the
        if error_type not in self.error_types:
            raise DBError("No error type with name %s is defined in the database" % error_type)

        # Generate baseline error message
        error_msg = PipelineError.error_msgs[error_type]

        # Add additional error message if necessary
        if extra_error_msg != "":
            error_msg += "\n%s" % extra_error_msg

        # Update the status of the pipeline
        pipeline.error_id   = self.error_types[error_type]
        pipeline.error_msg  = error_msg

    @staticmethod
    def pipeline_exists(session, pipeline_id):

        # Return true if database contains pipeline with the id specified
        pipelines = session.query(Analysis). \
            filter(Analysis.analysis_id == pipeline_id).all()

        return len(pipelines) > 0

    @staticmethod
    def register_output_file(pipeline, out_file):
        # Add an additonal output file to a pipeline in the database
        output = AnalysisOutput(node_id=out_file.get_node_id(),
                                output_key=out_file.get_filetype(),
                                path=out_file.get_path())
        pipeline.output.append(output)

    @staticmethod
    def get_config_file_strings(pipeline):
        # Get string representations of GAP config files
        config_strings  = {}
        config_strings["graph"]           = DBHelper.get_config_file(pipeline, "graph")
        config_strings["resource_kit"]    = DBHelper.get_config_file(pipeline, "resource_kit")
        config_strings["platform"]        = DBHelper.get_config_file(pipeline, "platform")
        config_strings["sample_sheet"]    = DBHelper.get_config_file(pipeline, "sample_sheet")
        config_strings["startup_script"]  = DBHelper.get_config_file(pipeline, "startup_script")
        return config_strings

    @staticmethod
    def get_config_file(pipeline, config_type):

        config = None

        if config_type == "graph":
            config = pipeline.analysis_type.graph_config.data

        elif config_type == "resource_kit":
            config = pipeline.analysis_type.resource_kit.data

        elif config_type == "platform":
            config = pipeline.analysis_type.platform_config.data

        elif config_type == "sample_sheet":
            config = pipeline.sample_sheet

        elif config_type == "startup_script":
            if pipeline.analysis_type.startup_script is not None:
                config = pipeline.analysis_type.startup_script.data
        else:
            logging.error("DBHelper attempted to get config file of type '%s'." % config_type)
            logging.error(
                "Must be of following type: 'graph', 'resource_kit', 'startup_script', 'platform', 'sample_sheet'.")
            raise DBError("Invalid config type requested from database: %s" % config_type)

        # Base64 decode if returned something
        if config is not None:
            config = base64.b64decode(config)

        return config

    @contextmanager
    def session_context(self):

        session = None

        try:

            # Obtain a new session
            session = self.session_factory()

            # Yield the session to the context
            yield session

            # Commit changes to database
            session.commit()

        except BaseException, e:

            # Log the error
            if e.message != "":
                logging.error("The following error appeared: %s. This database transaction will be rolled back." % e.message)
            else:
                logging.error("An database transaction error was encountered. The transaction will be rolled back.")

            if session is not None:
                session.rollback()

            raise

        finally:
            if session is not None:
                session.close()
