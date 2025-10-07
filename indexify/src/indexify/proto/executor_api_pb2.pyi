from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper

DESCRIPTOR: _descriptor.FileDescriptor

class DataPayloadEncoding(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DATA_PAYLOAD_ENCODING_UNKNOWN: _ClassVar[DataPayloadEncoding]
    DATA_PAYLOAD_ENCODING_UTF8_JSON: _ClassVar[DataPayloadEncoding]
    DATA_PAYLOAD_ENCODING_UTF8_TEXT: _ClassVar[DataPayloadEncoding]
    DATA_PAYLOAD_ENCODING_BINARY_PICKLE: _ClassVar[DataPayloadEncoding]
    DATA_PAYLOAD_ENCODING_BINARY_ZIP: _ClassVar[DataPayloadEncoding]
    DATA_PAYLOAD_ENCODING_RAW: _ClassVar[DataPayloadEncoding]

class GPUModel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GPU_MODEL_UNKNOWN: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_A100_40GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_A100_80GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_H100_80GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_TESLA_T4: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_A6000: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_A10: _ClassVar[GPUModel]

class FunctionExecutorStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FUNCTION_EXECUTOR_STATUS_UNKNOWN: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_PENDING: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_RUNNING: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_TERMINATED: _ClassVar[FunctionExecutorStatus]

class FunctionExecutorTerminationReason(
    int, metaclass=_enum_type_wrapper.EnumTypeWrapper
):
    __slots__ = ()
    FUNCTION_EXECUTOR_TERMINATION_REASON_UNKNOWN: _ClassVar[
        FunctionExecutorTerminationReason
    ]
    FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR: _ClassVar[
        FunctionExecutorTerminationReason
    ]
    FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR: _ClassVar[
        FunctionExecutorTerminationReason
    ]
    FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT: _ClassVar[
        FunctionExecutorTerminationReason
    ]
    FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY: _ClassVar[
        FunctionExecutorTerminationReason
    ]
    FUNCTION_EXECUTOR_TERMINATION_REASON_INTERNAL_ERROR: _ClassVar[
        FunctionExecutorTerminationReason
    ]
    FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_TIMEOUT: _ClassVar[
        FunctionExecutorTerminationReason
    ]
    FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED: _ClassVar[
        FunctionExecutorTerminationReason
    ]

class ExecutorStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXECUTOR_STATUS_UNKNOWN: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_STARTING_UP: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_RUNNING: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_DRAINED: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_STOPPED: _ClassVar[ExecutorStatus]

class AllocationOutcomeCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ALLOCATION_OUTCOME_CODE_UNKNOWN: _ClassVar[AllocationOutcomeCode]
    ALLOCATION_OUTCOME_CODE_SUCCESS: _ClassVar[AllocationOutcomeCode]
    ALLOCATION_OUTCOME_CODE_FAILURE: _ClassVar[AllocationOutcomeCode]

class AllocationFailureReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ALLOCATION_FAILURE_REASON_UNKNOWN: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_INTERNAL_ERROR: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_FUNCTION_ERROR: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_FUNCTION_TIMEOUT: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_REQUEST_ERROR: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_ALLOCATION_CANCELLED: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED: _ClassVar[
        AllocationFailureReason
    ]

DATA_PAYLOAD_ENCODING_UNKNOWN: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_UTF8_JSON: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_UTF8_TEXT: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_BINARY_PICKLE: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_BINARY_ZIP: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_RAW: DataPayloadEncoding
GPU_MODEL_UNKNOWN: GPUModel
GPU_MODEL_NVIDIA_A100_40GB: GPUModel
GPU_MODEL_NVIDIA_A100_80GB: GPUModel
GPU_MODEL_NVIDIA_H100_80GB: GPUModel
GPU_MODEL_NVIDIA_TESLA_T4: GPUModel
GPU_MODEL_NVIDIA_A6000: GPUModel
GPU_MODEL_NVIDIA_A10: GPUModel
FUNCTION_EXECUTOR_STATUS_UNKNOWN: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_PENDING: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_RUNNING: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_TERMINATED: FunctionExecutorStatus
FUNCTION_EXECUTOR_TERMINATION_REASON_UNKNOWN: FunctionExecutorTerminationReason
FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR: (
    FunctionExecutorTerminationReason
)
FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR: (
    FunctionExecutorTerminationReason
)
FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT: (
    FunctionExecutorTerminationReason
)
FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY: FunctionExecutorTerminationReason
FUNCTION_EXECUTOR_TERMINATION_REASON_INTERNAL_ERROR: FunctionExecutorTerminationReason
FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_TIMEOUT: FunctionExecutorTerminationReason
FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED: (
    FunctionExecutorTerminationReason
)
EXECUTOR_STATUS_UNKNOWN: ExecutorStatus
EXECUTOR_STATUS_STARTING_UP: ExecutorStatus
EXECUTOR_STATUS_RUNNING: ExecutorStatus
EXECUTOR_STATUS_DRAINED: ExecutorStatus
EXECUTOR_STATUS_STOPPED: ExecutorStatus
ALLOCATION_OUTCOME_CODE_UNKNOWN: AllocationOutcomeCode
ALLOCATION_OUTCOME_CODE_SUCCESS: AllocationOutcomeCode
ALLOCATION_OUTCOME_CODE_FAILURE: AllocationOutcomeCode
ALLOCATION_FAILURE_REASON_UNKNOWN: AllocationFailureReason
ALLOCATION_FAILURE_REASON_INTERNAL_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_FUNCTION_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_FUNCTION_TIMEOUT: AllocationFailureReason
ALLOCATION_FAILURE_REASON_REQUEST_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_ALLOCATION_CANCELLED: AllocationFailureReason
ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED: AllocationFailureReason

class DataPayload(_message.Message):
    __slots__ = (
        "uri",
        "encoding",
        "encoding_version",
        "content_type",
        "metadata_size",
        "offset",
        "size",
        "sha256_hash",
        "source_function_call_id",
        "id",
    )
    URI_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    ENCODING_VERSION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_SIZE_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    SHA256_HASH_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    uri: str
    encoding: DataPayloadEncoding
    encoding_version: int
    content_type: str
    metadata_size: int
    offset: int
    size: int
    sha256_hash: str
    source_function_call_id: str
    id: str
    def __init__(
        self,
        uri: _Optional[str] = ...,
        encoding: _Optional[_Union[DataPayloadEncoding, str]] = ...,
        encoding_version: _Optional[int] = ...,
        content_type: _Optional[str] = ...,
        metadata_size: _Optional[int] = ...,
        offset: _Optional[int] = ...,
        size: _Optional[int] = ...,
        sha256_hash: _Optional[str] = ...,
        source_function_call_id: _Optional[str] = ...,
        id: _Optional[str] = ...,
    ) -> None: ...

class GPUResources(_message.Message):
    __slots__ = ("count", "model")
    COUNT_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    count: int
    model: GPUModel
    def __init__(
        self, count: _Optional[int] = ..., model: _Optional[_Union[GPUModel, str]] = ...
    ) -> None: ...

class HostResources(_message.Message):
    __slots__ = ("cpu_count", "memory_bytes", "disk_bytes", "gpu")
    CPU_COUNT_FIELD_NUMBER: _ClassVar[int]
    MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    DISK_BYTES_FIELD_NUMBER: _ClassVar[int]
    GPU_FIELD_NUMBER: _ClassVar[int]
    cpu_count: int
    memory_bytes: int
    disk_bytes: int
    gpu: GPUResources
    def __init__(
        self,
        cpu_count: _Optional[int] = ...,
        memory_bytes: _Optional[int] = ...,
        disk_bytes: _Optional[int] = ...,
        gpu: _Optional[_Union[GPUResources, _Mapping]] = ...,
    ) -> None: ...

class AllowedFunction(_message.Message):
    __slots__ = (
        "namespace",
        "application_name",
        "function_name",
        "application_version",
    )
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_NAME_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_VERSION_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    application_name: str
    function_name: str
    application_version: str
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        application_name: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        application_version: _Optional[str] = ...,
    ) -> None: ...

class FunctionExecutorResources(_message.Message):
    __slots__ = ("cpu_ms_per_sec", "memory_bytes", "disk_bytes", "gpu")
    CPU_MS_PER_SEC_FIELD_NUMBER: _ClassVar[int]
    MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    DISK_BYTES_FIELD_NUMBER: _ClassVar[int]
    GPU_FIELD_NUMBER: _ClassVar[int]
    cpu_ms_per_sec: int
    memory_bytes: int
    disk_bytes: int
    gpu: GPUResources
    def __init__(
        self,
        cpu_ms_per_sec: _Optional[int] = ...,
        memory_bytes: _Optional[int] = ...,
        disk_bytes: _Optional[int] = ...,
        gpu: _Optional[_Union[GPUResources, _Mapping]] = ...,
    ) -> None: ...

class FunctionRef(_message.Message):
    __slots__ = (
        "namespace",
        "application_name",
        "function_name",
        "application_version",
    )
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_NAME_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_VERSION_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    application_name: str
    function_name: str
    application_version: str
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        application_name: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        application_version: _Optional[str] = ...,
    ) -> None: ...

class FunctionExecutorDescription(_message.Message):
    __slots__ = (
        "id",
        "function",
        "secret_names",
        "initialization_timeout_ms",
        "application",
        "resources",
        "output_payload_uri_prefix",
        "max_concurrency",
        "allocation_timeout_ms",
    )
    ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    SECRET_NAMES_FIELD_NUMBER: _ClassVar[int]
    INITIALIZATION_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PAYLOAD_URI_PREFIX_FIELD_NUMBER: _ClassVar[int]
    MAX_CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    id: str
    function: FunctionRef
    secret_names: _containers.RepeatedScalarFieldContainer[str]
    initialization_timeout_ms: int
    application: DataPayload
    resources: FunctionExecutorResources
    output_payload_uri_prefix: str
    max_concurrency: int
    allocation_timeout_ms: int
    def __init__(
        self,
        id: _Optional[str] = ...,
        function: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        secret_names: _Optional[_Iterable[str]] = ...,
        initialization_timeout_ms: _Optional[int] = ...,
        application: _Optional[_Union[DataPayload, _Mapping]] = ...,
        resources: _Optional[_Union[FunctionExecutorResources, _Mapping]] = ...,
        output_payload_uri_prefix: _Optional[str] = ...,
        max_concurrency: _Optional[int] = ...,
        allocation_timeout_ms: _Optional[int] = ...,
    ) -> None: ...

class FunctionExecutorState(_message.Message):
    __slots__ = (
        "description",
        "status",
        "termination_reason",
        "allocation_ids_caused_termination",
    )
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TERMINATION_REASON_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_IDS_CAUSED_TERMINATION_FIELD_NUMBER: _ClassVar[int]
    description: FunctionExecutorDescription
    status: FunctionExecutorStatus
    termination_reason: FunctionExecutorTerminationReason
    allocation_ids_caused_termination: _containers.RepeatedScalarFieldContainer[str]
    def __init__(
        self,
        description: _Optional[_Union[FunctionExecutorDescription, _Mapping]] = ...,
        status: _Optional[_Union[FunctionExecutorStatus, str]] = ...,
        termination_reason: _Optional[
            _Union[FunctionExecutorTerminationReason, str]
        ] = ...,
        allocation_ids_caused_termination: _Optional[_Iterable[str]] = ...,
    ) -> None: ...

class ExecutorState(_message.Message):
    __slots__ = (
        "executor_id",
        "hostname",
        "version",
        "status",
        "total_resources",
        "total_function_executor_resources",
        "allowed_functions",
        "function_executor_states",
        "labels",
        "state_hash",
        "server_clock",
    )

    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(
            self, key: _Optional[str] = ..., value: _Optional[str] = ...
        ) -> None: ...

    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FUNCTION_EXECUTOR_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_EXECUTOR_STATES_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    STATE_HASH_FIELD_NUMBER: _ClassVar[int]
    SERVER_CLOCK_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    hostname: str
    version: str
    status: ExecutorStatus
    total_resources: HostResources
    total_function_executor_resources: HostResources
    allowed_functions: _containers.RepeatedCompositeFieldContainer[AllowedFunction]
    function_executor_states: _containers.RepeatedCompositeFieldContainer[
        FunctionExecutorState
    ]
    labels: _containers.ScalarMap[str, str]
    state_hash: str
    server_clock: int
    def __init__(
        self,
        executor_id: _Optional[str] = ...,
        hostname: _Optional[str] = ...,
        version: _Optional[str] = ...,
        status: _Optional[_Union[ExecutorStatus, str]] = ...,
        total_resources: _Optional[_Union[HostResources, _Mapping]] = ...,
        total_function_executor_resources: _Optional[
            _Union[HostResources, _Mapping]
        ] = ...,
        allowed_functions: _Optional[
            _Iterable[_Union[AllowedFunction, _Mapping]]
        ] = ...,
        function_executor_states: _Optional[
            _Iterable[_Union[FunctionExecutorState, _Mapping]]
        ] = ...,
        labels: _Optional[_Mapping[str, str]] = ...,
        state_hash: _Optional[str] = ...,
        server_clock: _Optional[int] = ...,
    ) -> None: ...

class ExecutorUpdate(_message.Message):
    __slots__ = ("executor_id", "allocation_results")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_RESULTS_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    allocation_results: _containers.RepeatedCompositeFieldContainer[AllocationResult]
    def __init__(
        self,
        executor_id: _Optional[str] = ...,
        allocation_results: _Optional[
            _Iterable[_Union[AllocationResult, _Mapping]]
        ] = ...,
    ) -> None: ...

class ReportExecutorStateRequest(_message.Message):
    __slots__ = ("executor_state", "executor_update")
    EXECUTOR_STATE_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_UPDATE_FIELD_NUMBER: _ClassVar[int]
    executor_state: ExecutorState
    executor_update: ExecutorUpdate
    def __init__(
        self,
        executor_state: _Optional[_Union[ExecutorState, _Mapping]] = ...,
        executor_update: _Optional[_Union[ExecutorUpdate, _Mapping]] = ...,
    ) -> None: ...

class ReportExecutorStateResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Allocation(_message.Message):
    __slots__ = (
        "function",
        "allocation_id",
        "function_call_id",
        "request_id",
        "args",
        "output_payload_uri_prefix",
        "request_error_payload_uri_prefix",
        "function_executor_id",
        "function_call_metadata",
    )
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PAYLOAD_URI_PREFIX_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ERROR_PAYLOAD_URI_PREFIX_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_METADATA_FIELD_NUMBER: _ClassVar[int]
    function: FunctionRef
    allocation_id: str
    function_call_id: str
    request_id: str
    args: _containers.RepeatedCompositeFieldContainer[DataPayload]
    output_payload_uri_prefix: str
    request_error_payload_uri_prefix: str
    function_executor_id: str
    function_call_metadata: bytes
    def __init__(
        self,
        function: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        allocation_id: _Optional[str] = ...,
        function_call_id: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
        args: _Optional[_Iterable[_Union[DataPayload, _Mapping]]] = ...,
        output_payload_uri_prefix: _Optional[str] = ...,
        request_error_payload_uri_prefix: _Optional[str] = ...,
        function_executor_id: _Optional[str] = ...,
        function_call_metadata: _Optional[bytes] = ...,
    ) -> None: ...

class GetDesiredExecutorStatesRequest(_message.Message):
    __slots__ = ("executor_id",)
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    def __init__(self, executor_id: _Optional[str] = ...) -> None: ...

class DesiredExecutorState(_message.Message):
    __slots__ = ("function_executors", "allocations", "clock")
    FUNCTION_EXECUTORS_FIELD_NUMBER: _ClassVar[int]
    ALLOCATIONS_FIELD_NUMBER: _ClassVar[int]
    CLOCK_FIELD_NUMBER: _ClassVar[int]
    function_executors: _containers.RepeatedCompositeFieldContainer[
        FunctionExecutorDescription
    ]
    allocations: _containers.RepeatedCompositeFieldContainer[Allocation]
    clock: int
    def __init__(
        self,
        function_executors: _Optional[
            _Iterable[_Union[FunctionExecutorDescription, _Mapping]]
        ] = ...,
        allocations: _Optional[_Iterable[_Union[Allocation, _Mapping]]] = ...,
        clock: _Optional[int] = ...,
    ) -> None: ...

class ExecutionPlanUpdate(_message.Message):
    __slots__ = ("function_call", "reduce")
    FUNCTION_CALL_FIELD_NUMBER: _ClassVar[int]
    REDUCE_FIELD_NUMBER: _ClassVar[int]
    function_call: FunctionCall
    reduce: ReduceOp
    def __init__(
        self,
        function_call: _Optional[_Union[FunctionCall, _Mapping]] = ...,
        reduce: _Optional[_Union[ReduceOp, _Mapping]] = ...,
    ) -> None: ...

class FunctionCall(_message.Message):
    __slots__ = ("id", "target", "args", "call_metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    CALL_METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    target: FunctionRef
    args: _containers.RepeatedCompositeFieldContainer[FunctionArg]
    call_metadata: bytes
    def __init__(
        self,
        id: _Optional[str] = ...,
        target: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        args: _Optional[_Iterable[_Union[FunctionArg, _Mapping]]] = ...,
        call_metadata: _Optional[bytes] = ...,
    ) -> None: ...

class FunctionArg(_message.Message):
    __slots__ = ("function_call_id", "inline_data")
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    INLINE_DATA_FIELD_NUMBER: _ClassVar[int]
    function_call_id: str
    inline_data: DataPayload
    def __init__(
        self,
        function_call_id: _Optional[str] = ...,
        inline_data: _Optional[_Union[DataPayload, _Mapping]] = ...,
    ) -> None: ...

class ReduceOp(_message.Message):
    __slots__ = ("id", "collection", "reducer", "call_metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    REDUCER_FIELD_NUMBER: _ClassVar[int]
    CALL_METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    collection: _containers.RepeatedCompositeFieldContainer[FunctionArg]
    reducer: FunctionRef
    call_metadata: bytes
    def __init__(
        self,
        id: _Optional[str] = ...,
        collection: _Optional[_Iterable[_Union[FunctionArg, _Mapping]]] = ...,
        reducer: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        call_metadata: _Optional[bytes] = ...,
    ) -> None: ...

class ExecutionPlanUpdates(_message.Message):
    __slots__ = ("updates", "root_function_call_id")
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    ROOT_FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    updates: _containers.RepeatedCompositeFieldContainer[ExecutionPlanUpdate]
    root_function_call_id: str
    def __init__(
        self,
        updates: _Optional[_Iterable[_Union[ExecutionPlanUpdate, _Mapping]]] = ...,
        root_function_call_id: _Optional[str] = ...,
    ) -> None: ...

class AllocationResult(_message.Message):
    __slots__ = (
        "function",
        "allocation_id",
        "function_call_id",
        "request_id",
        "outcome_code",
        "failure_reason",
        "value",
        "updates",
        "request_error",
        "execution_duration_ms",
    )
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ERROR_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    function: FunctionRef
    allocation_id: str
    function_call_id: str
    request_id: str
    outcome_code: AllocationOutcomeCode
    failure_reason: AllocationFailureReason
    value: DataPayload
    updates: ExecutionPlanUpdates
    request_error: DataPayload
    execution_duration_ms: int
    def __init__(
        self,
        function: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        allocation_id: _Optional[str] = ...,
        function_call_id: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
        outcome_code: _Optional[_Union[AllocationOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[AllocationFailureReason, str]] = ...,
        value: _Optional[_Union[DataPayload, _Mapping]] = ...,
        updates: _Optional[_Union[ExecutionPlanUpdates, _Mapping]] = ...,
        request_error: _Optional[_Union[DataPayload, _Mapping]] = ...,
        execution_duration_ms: _Optional[int] = ...,
    ) -> None: ...

class FunctionCallRequest(_message.Message):
    __slots__ = (
        "parent_request_id",
        "function",
        "inputs",
        "call_metadata",
        "source_function_call_id",
    )
    PARENT_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    CALL_METADATA_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    parent_request_id: str
    function: FunctionRef
    inputs: _containers.RepeatedCompositeFieldContainer[DataPayload]
    call_metadata: bytes
    source_function_call_id: str
    def __init__(
        self,
        parent_request_id: _Optional[str] = ...,
        function: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        inputs: _Optional[_Iterable[_Union[DataPayload, _Mapping]]] = ...,
        call_metadata: _Optional[bytes] = ...,
        source_function_call_id: _Optional[str] = ...,
    ) -> None: ...

class FunctionCallResult(_message.Message):
    __slots__ = ("output", "exception")
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    output: DataPayload
    exception: str
    def __init__(
        self,
        output: _Optional[_Union[DataPayload, _Mapping]] = ...,
        exception: _Optional[str] = ...,
    ) -> None: ...

class FunctionCallResponse(_message.Message):
    __slots__ = ("update", "result")
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    update: str
    result: FunctionCallResult
    def __init__(
        self,
        update: _Optional[str] = ...,
        result: _Optional[_Union[FunctionCallResult, _Mapping]] = ...,
    ) -> None: ...
