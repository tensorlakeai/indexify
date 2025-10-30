from tensorlake.function_executor.proto.function_executor_pb2 import (
    ExecutionPlanUpdate as FEExecutionPlanUpdate,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    ExecutionPlanUpdates as FEExecutionPlanUpdates,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionArg as FEFunctionArg,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionCall as FEFunctionCall,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionRef as FEFunctionRef,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    ReduceOp as FEReduceOp,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObjectEncoding as FESerializedObjectEncoding,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObjectInsideBLOB as FESerializedObjectInsideBLOB,
)

from indexify.proto.executor_api_pb2 import DataPayload as ServerDataPayload
from indexify.proto.executor_api_pb2 import (
    DataPayloadEncoding as ServerDataPayloadEncoding,
)
from indexify.proto.executor_api_pb2 import (
    ExecutionPlanUpdate as ServerExecutionPlanUpdate,
)
from indexify.proto.executor_api_pb2 import (
    ExecutionPlanUpdates as ServerExecutionPlanUpdates,
)
from indexify.proto.executor_api_pb2 import FunctionArg as ServerFunctionArg
from indexify.proto.executor_api_pb2 import FunctionCall as ServerFunctionCall
from indexify.proto.executor_api_pb2 import FunctionRef as ServerFunctionRef
from indexify.proto.executor_api_pb2 import ReduceOp as ServerReduceOp


def to_server_execution_plan_updates(
    fe_execution_plan_updates: FEExecutionPlanUpdates, args_blob_uri: str | None
) -> ServerExecutionPlanUpdates:
    """Converts from Function Executor's ExecutionPlanUpdates to Server ExecutionPlanUpdates.

    Raises ValueError on validation error.
    args_blob_uri is required if FE execution plan update contains arguments for any function calls.
    """
    # TODO: Validate FEExecutionPlanUpdates object properly.
    server_execution_plan_updates: list[ServerExecutionPlanUpdate] = []
    for fe_update in fe_execution_plan_updates.updates:
        fe_update: FEExecutionPlanUpdate

        if fe_update.HasField("function_call"):
            server_execution_plan_updates.append(
                ServerExecutionPlanUpdate(
                    function_call=_to_server_function_call_proto(
                        fe_function_call=fe_update.function_call,
                        args_blob_uri=args_blob_uri,
                    )
                )
            )
        elif fe_update.HasField("reduce"):
            server_execution_plan_updates.append(
                ServerExecutionPlanUpdate(
                    reduce=_to_server_reduce_op_proto(
                        reduce=fe_update.reduce,
                        args_blob_uri=args_blob_uri,
                    )
                )
            )
        else:
            raise ValueError(
                "unexpected FEExecutionPlanUpdate with no function_call or reduce set",
            )

    return ServerExecutionPlanUpdates(
        updates=server_execution_plan_updates,
        root_function_call_id=fe_execution_plan_updates.root_function_call_id,
        start_at=(
            fe_execution_plan_updates.start_at
            if fe_execution_plan_updates.HasField("start_at")
            else None
        ),
    )


def _to_server_function_call_proto(
    fe_function_call: FEFunctionCall, args_blob_uri: str | None
) -> ServerFunctionCall:
    server_args: list[ServerFunctionArg] = []
    for fe_arg in fe_function_call.args:
        fe_arg: FEFunctionArg
        server_args.append(
            _to_server_function_arg_proto(
                fe_function_arg=fe_arg,
                args_blob_uri=args_blob_uri,
            )
        )

    return ServerFunctionCall(
        id=fe_function_call.id,
        target=_to_server_function_ref_proto(fe_function_call.target),
        args=server_args,
        call_metadata=fe_function_call.call_metadata,
    )


def _to_server_reduce_op_proto(
    reduce: FEReduceOp, args_blob_uri: str | None
) -> ServerReduceOp:
    collection: list[ServerFunctionArg] = []
    for fe_arg in reduce.collection:
        fe_arg: FEFunctionArg
        collection.append(
            _to_server_function_arg_proto(
                fe_function_arg=fe_arg,
                args_blob_uri=args_blob_uri,
            )
        )

    return ServerReduceOp(
        id=reduce.id,
        collection=collection,
        reducer=_to_server_function_ref_proto(reduce.reducer),
        call_metadata=reduce.call_metadata,
    )


def _to_server_function_ref_proto(fe_function_ref: FEFunctionRef) -> ServerFunctionRef:
    return ServerFunctionRef(
        namespace=fe_function_ref.namespace,
        application_name=fe_function_ref.application_name,
        function_name=fe_function_ref.function_name,
        application_version=fe_function_ref.application_version,
    )


def _to_server_function_arg_proto(
    fe_function_arg: FEFunctionArg, args_blob_uri: str | None
) -> ServerFunctionArg:
    if fe_function_arg.HasField("function_call_id"):
        return ServerFunctionArg(function_call_id=fe_function_arg.function_call_id)
    elif fe_function_arg.HasField("value"):
        return ServerFunctionArg(
            inline_data=_to_server_data_payload_proto(
                so=fe_function_arg.value,
                blob_uri=args_blob_uri,
            )
        )
    else:
        raise ValueError(
            "unexpected FEFunctionArg with no value or function_call_id set",
        )


def _to_server_data_payload_proto(
    so: FESerializedObjectInsideBLOB,
    blob_uri: str | None,
) -> ServerDataPayload:
    """Converts a serialized object inside BLOB to into a DataPayload."""
    if blob_uri is None:
        raise ValueError(
            "blob_uri is required to convert SerializedObjectInsideBLOB to DataPayload",
        )

    # TODO: Validate SerializedObjectInsideBLOB.
    return ServerDataPayload(
        uri=blob_uri,
        encoding=_to_server_data_payload_encoding(so.manifest.encoding),
        encoding_version=so.manifest.encoding_version,
        content_type=(
            so.manifest.content_type if so.manifest.HasField("content_type") else None
        ),
        metadata_size=so.manifest.metadata_size,
        offset=so.offset,
        size=so.manifest.size,
        sha256_hash=so.manifest.sha256_hash,
        source_function_call_id=so.manifest.source_function_call_id,
        # id is not used
    )


def _to_server_data_payload_encoding(
    encoding: FESerializedObjectEncoding,
) -> ServerDataPayloadEncoding:
    if encoding == FESerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE:
        return ServerDataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_PICKLE
    elif encoding == FESerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON:
        return ServerDataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_JSON
    elif encoding == FESerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT:
        return ServerDataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT
    elif encoding == FESerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW:
        return ServerDataPayloadEncoding.DATA_PAYLOAD_ENCODING_RAW
    else:
        raise ValueError(
            "unexpected encoding for SerializedObject",
            encoding=FESerializedObjectEncoding.Name(encoding),
        )
