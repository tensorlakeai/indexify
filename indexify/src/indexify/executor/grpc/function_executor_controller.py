from typing import Any

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    InitializeResponse,
    SerializedObject,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.proto.executor_api_pb2 import (
    FunctionExecutorDescription,
)
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorStatus as FunctionExecutorStatusProto,
)

from ..downloader import Downloader
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_status import FunctionExecutorStatus


class FunctionExecutorController:
    def __init__(
        self,
        function_executor_state: FunctionExecutorState,
        function_executor_description: FunctionExecutorDescription,
        downloader: Downloader,
        logger: Any,
    ):
        self._function_executor_state: FunctionExecutorState = function_executor_state
        self._function_executor_description: FunctionExecutorDescription = (
            function_executor_description
        )
        self._downloader: Downloader = downloader
        self._logger: Any = logger.bind(
            module=__name__,
            function_executor_id=function_executor_description.id,
            namespace=function_executor_description.namespace,
            graph_name=function_executor_description.graph_name,
            graph_version=function_executor_description.graph_version,
            function_name=function_executor_description.function_name,
            image_uri=function_executor_description.image_uri,
        )

    async def reconcile(self, desired_status: FunctionExecutorStatusProto) -> None:
        """Reconciles the current status with the desired status."""
        # TODO
        pass
