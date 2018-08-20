class PipelineError(object):
    NONE        = "NONE"
    INIT        = "INIT"
    LOAD        = "LOAD"
    RUN         = "RUN"
    REPORT      = "REPORT"
    CANCEL      = "CANCEL"
    OTHER       = "OTHER"

    # List types of errors
    error_types = [
        NONE,
        INIT,
        LOAD,
        RUN,
        REPORT,
        CANCEL,
        OTHER
    ]

    # Error message associated with each error type
    error_msgs = {
        NONE    : "No Error!",
        INIT    : "Error initializing GAP pipeline from database!",
        LOAD    : "Error loading pipeline runner platform!",
        RUN     : "GAP runtime error!",
        REPORT  : "GAP finished but report never received!",
        CANCEL  : "GAP job cancelled during runtime!",
        OTHER   : "Unexpected error!"
    }