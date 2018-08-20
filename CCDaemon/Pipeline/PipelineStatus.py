class PipelineStatus(object):
    IDLE        = "IDLE"
    READY       = "READY"
    LOADING     = "LOADING"
    RUNNING     = "RUNNING"
    CANCELLING  = "CANCELLING"
    DESTROYING  = "DESTROYING"
    FINISHED    = "FINISHED"
    SUCCESS     = "SUCCESS"
    FAILED      = "FAILED"

    status_list = [
        IDLE,
        READY,
        LOADING,
        RUNNING,
        CANCELLING,
        DESTROYING,
        FINISHED,
        SUCCESS,
        FAILED
    ]