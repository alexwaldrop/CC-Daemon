import logging
from configobj import *
from validate import Validator
from ConfigError import ConfigError

class ConfigParser(object):
    # Class for parsing, validating, and storing configuration information from external file
    def __init__(self, config, config_spec=None):

        # Parse config file as configobj
        self.config = self.parse(config, config_spec)

        # Validate config structure if config is specified
        if config_spec is not None:
            self.validate()

    def parse(self, config, config_spec):
        # Parse and return config data using ConfigObj
        try:

            return ConfigObj(config, configspec=config_spec, file_error=True)

        except IOError, e:
            logging.error("Missing config file! Received following error message:\n%s" % e.message)
            raise ConfigError(e.message)

        except BaseException, e:
            logging.error("Config parsing error!")
            if e.message != "":
                logging.error("Recieved the following error: %s" % e.message)
            raise ConfigError(e.message)

    def validate(self):
        # Validating schema
        validator = Validator()
        results = self.config.validate(validator, preserve_errors=True)

        # Reporting errors with file
        if results != True:
            error_string = "Invalid config error!\n"
            for (section_list, key, _) in flatten_errors(self.config, results):
                if key is not None:
                    error_string += '\tThe key "%s" in the section "%s" failed validation\n' % (
                    key, ', '.join(section_list))
                else:
                    logging.info('The following section was missing:%s \n' % (', '.join(section_list)))

            logging.error(error_string)

            self.valid = False
        else:
            self.valid = True

        if not self.valid:
            raise ConfigError("Config file did not pass validation against the spec file!")

    def get_config(self):
        return self.config
