from tensorlake.function_executor.proto.function_executor_pb2 import FunctionInputs


class AllocationInput:
    """Represents the input for a task allocation in the function executor controller."""

    def __init__(
        self,
        function_inputs: FunctionInputs,
        function_outputs_blob_uri: str,
        function_outputs_blob_upload_id: str,
        request_error_blob_uri: str,
        request_error_blob_upload_id: str,
    ):
        # Actual input object sent to FE.
        self.function_inputs = function_inputs
        # Executor side function input related bookkeeping.
        self.function_outputs_blob_uri = function_outputs_blob_uri
        self.function_outputs_blob_upload_id = function_outputs_blob_upload_id
        self.request_error_blob_uri = request_error_blob_uri
        self.request_error_blob_upload_id = request_error_blob_upload_id
