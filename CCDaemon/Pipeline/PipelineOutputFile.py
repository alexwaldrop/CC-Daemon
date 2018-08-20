class PipelineOutputFile:

    # Class for containing information about files produced by pipeline
    def __init__(self, filepath, filetype, node_id=None):
        self.path           = filepath
        self.filetype       = filetype
        self.node_id        = node_id
        self.path_exists    = False

    def get_path(self):
        return self.path

    def get_filetype(self):
        return self.filetype

    def get_node_id(self):
        return self.node_id

    def is_found(self):
        return self.path_exists

    def mark_as_found(self):
        self.path_exists = True

    def __str__(self):
        to_return = "Node: %s, Key: %s, Path: %s" % (self.node_id, self.filetype, self.path)
        return to_return
