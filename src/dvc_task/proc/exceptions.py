from ..exceptions import DvcTaskError


class ProcessNotTerminatedError(DvcTaskError):
    def __init__(self, name):
        super().__init__(f"Managed process '{name}' has not been terminated.")


class ProcessNotFoundError(DvcTaskError):
    def __init__(self, name):
        super().__init__(f"Managed process '{name}' does not exist.")


class TimeoutExpired(DvcTaskError):
    def __init__(self, cmd, timeout):
        super().__init__(
            f"'{cmd}' did not complete before timeout '{timeout}'"
        )
        self.cmd = cmd
        self.timeout = timeout


class UnsupportedSignalError(DvcTaskError):
    def __init__(self, sig):
        super().__init__(f"Unsupported signal: {sig}")
