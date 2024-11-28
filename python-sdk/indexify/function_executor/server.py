from indexify.http_client import IndexifyClient

from .handlers.run_function.handler import Handler as RunFunctionHandler
from .handlers.run_function.request_reader import (
    RequestReader as RunFunctionRequestReader,
)
from .handlers.run_function.response_writer import (
    ResponseWriter as RunFunctionResponseWriter,
)
from .protocol import RunFunctionRequest, RunFunctionResponse


class Server:
    def __init__(self, task_id: str, indexify_client: IndexifyClient):
        self._task_id = task_id
        self._indexify_client = indexify_client

    def run(self):
        """Runs Function Executor server.

        Currently it just runs RunFunctionRequest handler once and exits with return code indicating success or failure.
        This would change in the future once this gets converted into a proper RPC server.
        """
        # Customer function code never raises an exception because we catch all of them and add their details to the response.
        # We can only get an exception here if our own code failed. We don't catch the exception so it will bubble up and result
        # in a non-zero return code of our process and exception details available in our process stderr. This is the contract
        # the parent Executor process expects from us.
        request: RunFunctionRequest = RunFunctionRequestReader(self._task_id).read()
        response: RunFunctionResponse = RunFunctionHandler(
            request=request, indexify_client=self._indexify_client
        ).run()
        RunFunctionResponseWriter(response).write()
