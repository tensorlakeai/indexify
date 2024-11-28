import jsonpickle

from indexify.executor.paths.host import HostPaths
from indexify.function_executor.protocol import RunFunctionRequest

from .file_utils import bytes_from_file


class RequestReader:
    def __init__(self, task_id: str):
        self._task_id = task_id

    def read(self) -> RunFunctionRequest:
        serialized_request: bytes = bytes_from_file(
            HostPaths.task_run_function_request(self._task_id)
        )
        return jsonpickle.decode(serialized_request.decode("utf-8"))
