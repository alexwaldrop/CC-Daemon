class ConfigError(Exception):
    def __init__(self, *args, **kwargs):
        super(ConfigError, self).__init__(*args, **kwargs)