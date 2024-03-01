class FileNotFoundError(Exception):
    def __init__(self, message, errors):

        message_errors = f"{message}." + "\n" + "\n".join(errors)

        super.__init__(message_errors)