
class DBError(Exception):
    def __init__(self, *args, **kwargs):
        super(DBError, self).__init__(*args, **kwargs)
