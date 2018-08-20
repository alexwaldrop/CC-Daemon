import logging
from copy import deepcopy

from Config import Validatable

class PlatformFactory(Validatable):

    def __init__(self, config, platform_class):

        # Read and validate config
        super(PlatformFactory, self).__init__(config)

        # Class of platform that will be produced
        self.platform_class = platform_class

    def is_valid(self):
        logging.info("Validating Platform factory by creating TestPlatform...")
        test_platform = self.get_platform(name="TestPlatform")
        return test_platform.is_valid()

    def define_config_schema(self):
        return None

    def get_platform(self, name, **kwargs):
        config_copy = deepcopy(self.config)
        return self.platform_class(name=name, config=config_copy, **kwargs)

