class FastKmException(Exception):
    """Custom exception class for Fast-KM errors. The error message is intended to be user-facing (i.e., does not contain sensitive information)."""
    def __init__(self, user_facing_error: str):
        user_facing_error = f"-BEGIN USER-FACING ERROR- {user_facing_error} -END USER-FACING ERROR-"
        super().__init__(user_facing_error)