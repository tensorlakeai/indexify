import jsonpickle

from indexify.executor.paths.host import HostPaths
from indexify.function_executor.protocol import RunFunctionResponse

from .file_utils import write_to_file


class ResponseWriter:
    """Writes the result of a function or router call to the host filesystem."""

    def __init__(self, response: RunFunctionResponse):
        self._response = response

    def write(self):
        serialized_response: bytes = jsonpickle.encode(self._response).encode("utf-8")
        response_path = HostPaths.task_run_function_response(self._response.task_id)
        write_to_file(response_path, serialized_response)
