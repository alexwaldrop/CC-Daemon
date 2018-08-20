import abc

from Config import ConfigParser

class Validatable(object):
    # Abstract base class for objects that are initialized from configs and are validateable
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        # Parse and validate component config
        self.config_schema      = self.define_config_schema()
        self.config             = ConfigParser(config, self.config_schema).get_config()

    @abc.abstractmethod
    def is_valid(self):
        # Returns true if object currently has valid configuration, false otherwise
        # Inheriting classes decided how to determine whether object configuration is valid
        pass

    @abc.abstractmethod
    def define_config_schema(self):
        # Return an object that can be used to validate the config
        pass
