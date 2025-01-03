class ApiException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class GraphStillProcessing(Exception):
    def __init__(self) -> None:
        super().__init__("graph is still processing")
