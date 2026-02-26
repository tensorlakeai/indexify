import datetime
from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import timestamp_pb2 as _timestamp_pb2
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

class ContainerStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONTAINER_STATUS_UNKNOWN: _ClassVar[ContainerStatus]
    CONTAINER_STATUS_PENDING: _ClassVar[ContainerStatus]
    CONTAINER_STATUS_RUNNING: _ClassVar[ContainerStatus]
    CONTAINER_STATUS_TERMINATED: _ClassVar[ContainerStatus]

class ContainerType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONTAINER_TYPE_UNKNOWN: _ClassVar[ContainerType]
    CONTAINER_TYPE_FUNCTION: _ClassVar[ContainerType]
    CONTAINER_TYPE_SANDBOX: _ClassVar[ContainerType]

class ExecutorStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXECUTOR_STATUS_UNKNOWN: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_STARTING_UP: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_RUNNING: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_DRAINED: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_STOPPED: _ClassVar[ExecutorStatus]

class ReplayMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REPLAY_MODE_NONE: _ClassVar[ReplayMode]
    REPLAY_MODE_STRICT: _ClassVar[ReplayMode]
    REPLAY_MODE_ADAPTIVE: _ClassVar[ReplayMode]

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
    ALLOCATION_FAILURE_REASON_CONTAINER_TERMINATED: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_OOM: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_CONSTRAINT_UNSATISFIABLE: _ClassVar[
        AllocationFailureReason
    ]
    ALLOCATION_FAILURE_REASON_EXECUTOR_REMOVED: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_STARTUP_FAILED_INTERNAL_ERROR: _ClassVar[
        AllocationFailureReason
    ]
    ALLOCATION_FAILURE_REASON_STARTUP_FAILED_FUNCTION_ERROR: _ClassVar[
        AllocationFailureReason
    ]
    ALLOCATION_FAILURE_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT: _ClassVar[
        AllocationFailureReason
    ]
    ALLOCATION_FAILURE_REASON_STARTUP_FAILED_BAD_IMAGE: _ClassVar[
        AllocationFailureReason
    ]

class ContainerTerminationReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONTAINER_TERMINATION_REASON_UNKNOWN: _ClassVar[ContainerTerminationReason]
    CONTAINER_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR: _ClassVar[
        ContainerTerminationReason
    ]
    CONTAINER_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR: _ClassVar[
        ContainerTerminationReason
    ]
    CONTAINER_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT: _ClassVar[
        ContainerTerminationReason
    ]
    CONTAINER_TERMINATION_REASON_UNHEALTHY: _ClassVar[ContainerTerminationReason]
    CONTAINER_TERMINATION_REASON_INTERNAL_ERROR: _ClassVar[ContainerTerminationReason]
    CONTAINER_TERMINATION_REASON_FUNCTION_TIMEOUT: _ClassVar[ContainerTerminationReason]
    CONTAINER_TERMINATION_REASON_FUNCTION_CANCELLED: _ClassVar[
        ContainerTerminationReason
    ]
    CONTAINER_TERMINATION_REASON_OOM: _ClassVar[ContainerTerminationReason]
    CONTAINER_TERMINATION_REASON_PROCESS_CRASH: _ClassVar[ContainerTerminationReason]
    CONTAINER_TERMINATION_REASON_STARTUP_FAILED_BAD_IMAGE: _ClassVar[
        ContainerTerminationReason
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
CONTAINER_STATUS_UNKNOWN: ContainerStatus
CONTAINER_STATUS_PENDING: ContainerStatus
CONTAINER_STATUS_RUNNING: ContainerStatus
CONTAINER_STATUS_TERMINATED: ContainerStatus
CONTAINER_TYPE_UNKNOWN: ContainerType
CONTAINER_TYPE_FUNCTION: ContainerType
CONTAINER_TYPE_SANDBOX: ContainerType
EXECUTOR_STATUS_UNKNOWN: ExecutorStatus
EXECUTOR_STATUS_STARTING_UP: ExecutorStatus
EXECUTOR_STATUS_RUNNING: ExecutorStatus
EXECUTOR_STATUS_DRAINED: ExecutorStatus
EXECUTOR_STATUS_STOPPED: ExecutorStatus
REPLAY_MODE_NONE: ReplayMode
REPLAY_MODE_STRICT: ReplayMode
REPLAY_MODE_ADAPTIVE: ReplayMode
ALLOCATION_OUTCOME_CODE_UNKNOWN: AllocationOutcomeCode
ALLOCATION_OUTCOME_CODE_SUCCESS: AllocationOutcomeCode
ALLOCATION_OUTCOME_CODE_FAILURE: AllocationOutcomeCode
ALLOCATION_FAILURE_REASON_UNKNOWN: AllocationFailureReason
ALLOCATION_FAILURE_REASON_INTERNAL_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_FUNCTION_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_FUNCTION_TIMEOUT: AllocationFailureReason
ALLOCATION_FAILURE_REASON_REQUEST_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_ALLOCATION_CANCELLED: AllocationFailureReason
ALLOCATION_FAILURE_REASON_CONTAINER_TERMINATED: AllocationFailureReason
ALLOCATION_FAILURE_REASON_OOM: AllocationFailureReason
ALLOCATION_FAILURE_REASON_CONSTRAINT_UNSATISFIABLE: AllocationFailureReason
ALLOCATION_FAILURE_REASON_EXECUTOR_REMOVED: AllocationFailureReason
ALLOCATION_FAILURE_REASON_STARTUP_FAILED_INTERNAL_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_STARTUP_FAILED_FUNCTION_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT: AllocationFailureReason
ALLOCATION_FAILURE_REASON_STARTUP_FAILED_BAD_IMAGE: AllocationFailureReason
CONTAINER_TERMINATION_REASON_UNKNOWN: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_UNHEALTHY: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_INTERNAL_ERROR: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_FUNCTION_TIMEOUT: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_FUNCTION_CANCELLED: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_OOM: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_PROCESS_CRASH: ContainerTerminationReason
CONTAINER_TERMINATION_REASON_STARTUP_FAILED_BAD_IMAGE: ContainerTerminationReason

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

class ContainerResources(_message.Message):
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

class NetworkPolicy(_message.Message):
    __slots__ = ("allow_internet_access", "allow_out", "deny_out")
    ALLOW_INTERNET_ACCESS_FIELD_NUMBER: _ClassVar[int]
    ALLOW_OUT_FIELD_NUMBER: _ClassVar[int]
    DENY_OUT_FIELD_NUMBER: _ClassVar[int]
    allow_internet_access: bool
    allow_out: _containers.RepeatedScalarFieldContainer[str]
    deny_out: _containers.RepeatedScalarFieldContainer[str]
    def __init__(
        self,
        allow_internet_access: bool = ...,
        allow_out: _Optional[_Iterable[str]] = ...,
        deny_out: _Optional[_Iterable[str]] = ...,
    ) -> None: ...

class SandboxMetadata(_message.Message):
    __slots__ = (
        "timeout_secs",
        "entrypoint",
        "image",
        "network_policy",
        "sandbox_id",
        "snapshot_uri",
    )
    TIMEOUT_SECS_FIELD_NUMBER: _ClassVar[int]
    ENTRYPOINT_FIELD_NUMBER: _ClassVar[int]
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    NETWORK_POLICY_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_URI_FIELD_NUMBER: _ClassVar[int]
    timeout_secs: int
    entrypoint: _containers.RepeatedScalarFieldContainer[str]
    image: str
    network_policy: NetworkPolicy
    sandbox_id: str
    snapshot_uri: str
    def __init__(
        self,
        timeout_secs: _Optional[int] = ...,
        entrypoint: _Optional[_Iterable[str]] = ...,
        image: _Optional[str] = ...,
        network_policy: _Optional[_Union[NetworkPolicy, _Mapping]] = ...,
        sandbox_id: _Optional[str] = ...,
        snapshot_uri: _Optional[str] = ...,
    ) -> None: ...

class ContainerDescription(_message.Message):
    __slots__ = (
        "id",
        "function",
        "secret_names",
        "initialization_timeout_ms",
        "application",
        "resources",
        "max_concurrency",
        "allocation_timeout_ms",
        "sandbox_metadata",
        "container_type",
        "pool_id",
    )
    ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    SECRET_NAMES_FIELD_NUMBER: _ClassVar[int]
    INITIALIZATION_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    MAX_CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_METADATA_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_TYPE_FIELD_NUMBER: _ClassVar[int]
    POOL_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    function: FunctionRef
    secret_names: _containers.RepeatedScalarFieldContainer[str]
    initialization_timeout_ms: int
    application: DataPayload
    resources: ContainerResources
    max_concurrency: int
    allocation_timeout_ms: int
    sandbox_metadata: SandboxMetadata
    container_type: ContainerType
    pool_id: str
    def __init__(
        self,
        id: _Optional[str] = ...,
        function: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        secret_names: _Optional[_Iterable[str]] = ...,
        initialization_timeout_ms: _Optional[int] = ...,
        application: _Optional[_Union[DataPayload, _Mapping]] = ...,
        resources: _Optional[_Union[ContainerResources, _Mapping]] = ...,
        max_concurrency: _Optional[int] = ...,
        allocation_timeout_ms: _Optional[int] = ...,
        sandbox_metadata: _Optional[_Union[SandboxMetadata, _Mapping]] = ...,
        container_type: _Optional[_Union[ContainerType, str]] = ...,
        pool_id: _Optional[str] = ...,
    ) -> None: ...

class ContainerState(_message.Message):
    __slots__ = ("description", "status", "termination_reason")
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TERMINATION_REASON_FIELD_NUMBER: _ClassVar[int]
    description: ContainerDescription
    status: ContainerStatus
    termination_reason: ContainerTerminationReason
    def __init__(
        self,
        description: _Optional[_Union[ContainerDescription, _Mapping]] = ...,
        status: _Optional[_Union[ContainerStatus, str]] = ...,
        termination_reason: _Optional[_Union[ContainerTerminationReason, str]] = ...,
    ) -> None: ...

class ExecutorState(_message.Message):
    __slots__ = (
        "executor_id",
        "hostname",
        "version",
        "status",
        "total_resources",
        "total_container_resources",
        "allowed_functions",
        "container_states",
        "labels",
        "state_hash",
        "server_clock",
        "catalog_entry_name",
        "proxy_address",
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
    TOTAL_CONTAINER_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_STATES_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    STATE_HASH_FIELD_NUMBER: _ClassVar[int]
    SERVER_CLOCK_FIELD_NUMBER: _ClassVar[int]
    CATALOG_ENTRY_NAME_FIELD_NUMBER: _ClassVar[int]
    PROXY_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    hostname: str
    version: str
    status: ExecutorStatus
    total_resources: HostResources
    total_container_resources: HostResources
    allowed_functions: _containers.RepeatedCompositeFieldContainer[AllowedFunction]
    container_states: _containers.RepeatedCompositeFieldContainer[ContainerState]
    labels: _containers.ScalarMap[str, str]
    state_hash: str
    server_clock: int
    catalog_entry_name: str
    proxy_address: str
    def __init__(
        self,
        executor_id: _Optional[str] = ...,
        hostname: _Optional[str] = ...,
        version: _Optional[str] = ...,
        status: _Optional[_Union[ExecutorStatus, str]] = ...,
        total_resources: _Optional[_Union[HostResources, _Mapping]] = ...,
        total_container_resources: _Optional[_Union[HostResources, _Mapping]] = ...,
        allowed_functions: _Optional[
            _Iterable[_Union[AllowedFunction, _Mapping]]
        ] = ...,
        container_states: _Optional[_Iterable[_Union[ContainerState, _Mapping]]] = ...,
        labels: _Optional[_Mapping[str, str]] = ...,
        state_hash: _Optional[str] = ...,
        server_clock: _Optional[int] = ...,
        catalog_entry_name: _Optional[str] = ...,
        proxy_address: _Optional[str] = ...,
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

class Allocation(_message.Message):
    __slots__ = (
        "function",
        "allocation_id",
        "function_call_id",
        "request_id",
        "args",
        "request_data_payload_uri_prefix",
        "request_error_payload_uri_prefix",
        "container_id",
        "function_call_metadata",
        "replay_mode",
        "last_event_clock",
    )
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_DATA_PAYLOAD_URI_PREFIX_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ERROR_PAYLOAD_URI_PREFIX_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_METADATA_FIELD_NUMBER: _ClassVar[int]
    REPLAY_MODE_FIELD_NUMBER: _ClassVar[int]
    LAST_EVENT_CLOCK_FIELD_NUMBER: _ClassVar[int]
    function: FunctionRef
    allocation_id: str
    function_call_id: str
    request_id: str
    args: _containers.RepeatedCompositeFieldContainer[DataPayload]
    request_data_payload_uri_prefix: str
    request_error_payload_uri_prefix: str
    container_id: str
    function_call_metadata: bytes
    replay_mode: ReplayMode
    last_event_clock: int
    def __init__(
        self,
        function: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        allocation_id: _Optional[str] = ...,
        function_call_id: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
        args: _Optional[_Iterable[_Union[DataPayload, _Mapping]]] = ...,
        request_data_payload_uri_prefix: _Optional[str] = ...,
        request_error_payload_uri_prefix: _Optional[str] = ...,
        container_id: _Optional[str] = ...,
        function_call_metadata: _Optional[bytes] = ...,
        replay_mode: _Optional[_Union[ReplayMode, str]] = ...,
        last_event_clock: _Optional[int] = ...,
    ) -> None: ...

class FunctionCallResult(_message.Message):
    __slots__ = (
        "namespace",
        "request_id",
        "function_call_id",
        "outcome_code",
        "failure_reason",
        "return_value",
        "request_error",
    )
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    RETURN_VALUE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ERROR_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    request_id: str
    function_call_id: str
    outcome_code: AllocationOutcomeCode
    failure_reason: AllocationFailureReason
    return_value: DataPayload
    request_error: DataPayload
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
        function_call_id: _Optional[str] = ...,
        outcome_code: _Optional[_Union[AllocationOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[AllocationFailureReason, str]] = ...,
        return_value: _Optional[_Union[DataPayload, _Mapping]] = ...,
        request_error: _Optional[_Union[DataPayload, _Mapping]] = ...,
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
    __slots__ = ("updates", "root_function_call_id", "start_at")
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    ROOT_FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    START_AT_FIELD_NUMBER: _ClassVar[int]
    updates: _containers.RepeatedCompositeFieldContainer[ExecutionPlanUpdate]
    root_function_call_id: str
    start_at: _timestamp_pb2.Timestamp
    def __init__(
        self,
        updates: _Optional[_Iterable[_Union[ExecutionPlanUpdate, _Mapping]]] = ...,
        root_function_call_id: _Optional[str] = ...,
        start_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
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
        "namespace",
        "application",
        "request_id",
        "updates",
        "source_function_call_id",
    )
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    application: str
    request_id: str
    updates: ExecutionPlanUpdates
    source_function_call_id: str
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        application: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
        updates: _Optional[_Union[ExecutionPlanUpdates, _Mapping]] = ...,
        source_function_call_id: _Optional[str] = ...,
    ) -> None: ...

class FunctionCallResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = (
        "executor_id",
        "status",
        "full_state",
        "command_responses",
        "allocation_outcomes",
        "allocation_log_entries",
    )
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FULL_STATE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_RESPONSES_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_OUTCOMES_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_LOG_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    status: ExecutorStatus
    full_state: DataplaneStateFullSync
    command_responses: _containers.RepeatedCompositeFieldContainer[CommandResponse]
    allocation_outcomes: _containers.RepeatedCompositeFieldContainer[AllocationOutcome]
    allocation_log_entries: _containers.RepeatedCompositeFieldContainer[
        AllocationLogEntry
    ]
    def __init__(
        self,
        executor_id: _Optional[str] = ...,
        status: _Optional[_Union[ExecutorStatus, str]] = ...,
        full_state: _Optional[_Union[DataplaneStateFullSync, _Mapping]] = ...,
        command_responses: _Optional[
            _Iterable[_Union[CommandResponse, _Mapping]]
        ] = ...,
        allocation_outcomes: _Optional[
            _Iterable[_Union[AllocationOutcome, _Mapping]]
        ] = ...,
        allocation_log_entries: _Optional[
            _Iterable[_Union[AllocationLogEntry, _Mapping]]
        ] = ...,
    ) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ("send_state",)
    SEND_STATE_FIELD_NUMBER: _ClassVar[int]
    send_state: bool
    def __init__(self, send_state: bool = ...) -> None: ...

class DataplaneStateFullSync(_message.Message):
    __slots__ = (
        "hostname",
        "version",
        "total_resources",
        "total_container_resources",
        "allowed_functions",
        "labels",
        "catalog_entry_name",
        "proxy_address",
        "container_states",
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

    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    TOTAL_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CONTAINER_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    CATALOG_ENTRY_NAME_FIELD_NUMBER: _ClassVar[int]
    PROXY_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_STATES_FIELD_NUMBER: _ClassVar[int]
    hostname: str
    version: str
    total_resources: HostResources
    total_container_resources: HostResources
    allowed_functions: _containers.RepeatedCompositeFieldContainer[AllowedFunction]
    labels: _containers.ScalarMap[str, str]
    catalog_entry_name: str
    proxy_address: str
    container_states: _containers.RepeatedCompositeFieldContainer[ContainerState]
    def __init__(
        self,
        hostname: _Optional[str] = ...,
        version: _Optional[str] = ...,
        total_resources: _Optional[_Union[HostResources, _Mapping]] = ...,
        total_container_resources: _Optional[_Union[HostResources, _Mapping]] = ...,
        allowed_functions: _Optional[
            _Iterable[_Union[AllowedFunction, _Mapping]]
        ] = ...,
        labels: _Optional[_Mapping[str, str]] = ...,
        catalog_entry_name: _Optional[str] = ...,
        proxy_address: _Optional[str] = ...,
        container_states: _Optional[_Iterable[_Union[ContainerState, _Mapping]]] = ...,
    ) -> None: ...

class ContainerStateUpdate(_message.Message):
    __slots__ = ("container_id", "status", "termination_reason")
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TERMINATION_REASON_FIELD_NUMBER: _ClassVar[int]
    container_id: str
    status: ContainerStatus
    termination_reason: ContainerTerminationReason
    def __init__(
        self,
        container_id: _Optional[str] = ...,
        status: _Optional[_Union[ContainerStatus, str]] = ...,
        termination_reason: _Optional[_Union[ContainerTerminationReason, str]] = ...,
    ) -> None: ...

class Command(_message.Message):
    __slots__ = (
        "seq",
        "add_container",
        "remove_container",
        "run_allocation",
        "kill_allocation",
        "update_container_description",
        "snapshot_container",
    )
    SEQ_FIELD_NUMBER: _ClassVar[int]
    ADD_CONTAINER_FIELD_NUMBER: _ClassVar[int]
    REMOVE_CONTAINER_FIELD_NUMBER: _ClassVar[int]
    RUN_ALLOCATION_FIELD_NUMBER: _ClassVar[int]
    KILL_ALLOCATION_FIELD_NUMBER: _ClassVar[int]
    UPDATE_CONTAINER_DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_CONTAINER_FIELD_NUMBER: _ClassVar[int]
    seq: int
    add_container: AddContainer
    remove_container: RemoveContainer
    run_allocation: RunAllocation
    kill_allocation: KillAllocation
    update_container_description: UpdateContainerDescription
    snapshot_container: SnapshotContainer
    def __init__(
        self,
        seq: _Optional[int] = ...,
        add_container: _Optional[_Union[AddContainer, _Mapping]] = ...,
        remove_container: _Optional[_Union[RemoveContainer, _Mapping]] = ...,
        run_allocation: _Optional[_Union[RunAllocation, _Mapping]] = ...,
        kill_allocation: _Optional[_Union[KillAllocation, _Mapping]] = ...,
        update_container_description: _Optional[
            _Union[UpdateContainerDescription, _Mapping]
        ] = ...,
        snapshot_container: _Optional[_Union[SnapshotContainer, _Mapping]] = ...,
    ) -> None: ...

class SnapshotContainer(_message.Message):
    __slots__ = ("container_id", "snapshot_id", "upload_uri")
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_URI_FIELD_NUMBER: _ClassVar[int]
    container_id: str
    snapshot_id: str
    upload_uri: str
    def __init__(
        self,
        container_id: _Optional[str] = ...,
        snapshot_id: _Optional[str] = ...,
        upload_uri: _Optional[str] = ...,
    ) -> None: ...

class UpdateContainerDescription(_message.Message):
    __slots__ = ("container_id", "sandbox_metadata")
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_METADATA_FIELD_NUMBER: _ClassVar[int]
    container_id: str
    sandbox_metadata: SandboxMetadata
    def __init__(
        self,
        container_id: _Optional[str] = ...,
        sandbox_metadata: _Optional[_Union[SandboxMetadata, _Mapping]] = ...,
    ) -> None: ...

class AddContainer(_message.Message):
    __slots__ = ("container",)
    CONTAINER_FIELD_NUMBER: _ClassVar[int]
    container: ContainerDescription
    def __init__(
        self, container: _Optional[_Union[ContainerDescription, _Mapping]] = ...
    ) -> None: ...

class RemoveContainer(_message.Message):
    __slots__ = ("container_id", "reason")
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    container_id: str
    reason: ContainerTerminationReason
    def __init__(
        self,
        container_id: _Optional[str] = ...,
        reason: _Optional[_Union[ContainerTerminationReason, str]] = ...,
    ) -> None: ...

class RunAllocation(_message.Message):
    __slots__ = ("allocation",)
    ALLOCATION_FIELD_NUMBER: _ClassVar[int]
    allocation: Allocation
    def __init__(
        self, allocation: _Optional[_Union[Allocation, _Mapping]] = ...
    ) -> None: ...

class KillAllocation(_message.Message):
    __slots__ = ("allocation_id",)
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    allocation_id: str
    def __init__(self, allocation_id: _Optional[str] = ...) -> None: ...

class CommandResponse(_message.Message):
    __slots__ = (
        "command_seq",
        "container_started",
        "container_terminated",
        "allocation_scheduled",
        "snapshot_completed",
        "snapshot_failed",
    )
    COMMAND_SEQ_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_STARTED_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_TERMINATED_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_SCHEDULED_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FAILED_FIELD_NUMBER: _ClassVar[int]
    command_seq: int
    container_started: ContainerStarted
    container_terminated: ContainerTerminated
    allocation_scheduled: AllocationScheduled
    snapshot_completed: SnapshotCompleted
    snapshot_failed: SnapshotFailed
    def __init__(
        self,
        command_seq: _Optional[int] = ...,
        container_started: _Optional[_Union[ContainerStarted, _Mapping]] = ...,
        container_terminated: _Optional[_Union[ContainerTerminated, _Mapping]] = ...,
        allocation_scheduled: _Optional[_Union[AllocationScheduled, _Mapping]] = ...,
        snapshot_completed: _Optional[_Union[SnapshotCompleted, _Mapping]] = ...,
        snapshot_failed: _Optional[_Union[SnapshotFailed, _Mapping]] = ...,
    ) -> None: ...

class SnapshotCompleted(_message.Message):
    __slots__ = ("container_id", "snapshot_id", "snapshot_uri", "size_bytes")
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_URI_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    container_id: str
    snapshot_id: str
    snapshot_uri: str
    size_bytes: int
    def __init__(
        self,
        container_id: _Optional[str] = ...,
        snapshot_id: _Optional[str] = ...,
        snapshot_uri: _Optional[str] = ...,
        size_bytes: _Optional[int] = ...,
    ) -> None: ...

class SnapshotFailed(_message.Message):
    __slots__ = ("container_id", "snapshot_id", "error_message")
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    container_id: str
    snapshot_id: str
    error_message: str
    def __init__(
        self,
        container_id: _Optional[str] = ...,
        snapshot_id: _Optional[str] = ...,
        error_message: _Optional[str] = ...,
    ) -> None: ...

class AllocationScheduled(_message.Message):
    __slots__ = ("allocation_id",)
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    allocation_id: str
    def __init__(self, allocation_id: _Optional[str] = ...) -> None: ...

class ContainerStarted(_message.Message):
    __slots__ = ("container_id",)
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    container_id: str
    def __init__(self, container_id: _Optional[str] = ...) -> None: ...

class ContainerTerminated(_message.Message):
    __slots__ = ("container_id", "reason")
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    container_id: str
    reason: ContainerTerminationReason
    def __init__(
        self,
        container_id: _Optional[str] = ...,
        reason: _Optional[_Union[ContainerTerminationReason, str]] = ...,
    ) -> None: ...

class AllocationCompleted(_message.Message):
    __slots__ = (
        "allocation_id",
        "function",
        "function_call_id",
        "request_id",
        "value",
        "updates",
        "execution_duration_ms",
    )
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    allocation_id: str
    function: FunctionRef
    function_call_id: str
    request_id: str
    value: DataPayload
    updates: ExecutionPlanUpdates
    execution_duration_ms: int
    def __init__(
        self,
        allocation_id: _Optional[str] = ...,
        function: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        function_call_id: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
        value: _Optional[_Union[DataPayload, _Mapping]] = ...,
        updates: _Optional[_Union[ExecutionPlanUpdates, _Mapping]] = ...,
        execution_duration_ms: _Optional[int] = ...,
    ) -> None: ...

class AllocationFailed(_message.Message):
    __slots__ = (
        "allocation_id",
        "reason",
        "function",
        "function_call_id",
        "request_id",
        "request_error",
        "execution_duration_ms",
        "container_id",
    )
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ERROR_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_ID_FIELD_NUMBER: _ClassVar[int]
    allocation_id: str
    reason: AllocationFailureReason
    function: FunctionRef
    function_call_id: str
    request_id: str
    request_error: DataPayload
    execution_duration_ms: int
    container_id: str
    def __init__(
        self,
        allocation_id: _Optional[str] = ...,
        reason: _Optional[_Union[AllocationFailureReason, str]] = ...,
        function: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        function_call_id: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
        request_error: _Optional[_Union[DataPayload, _Mapping]] = ...,
        execution_duration_ms: _Optional[int] = ...,
        container_id: _Optional[str] = ...,
    ) -> None: ...

class AllocationOutcome(_message.Message):
    __slots__ = ("completed", "failed")
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    completed: AllocationCompleted
    failed: AllocationFailed
    def __init__(
        self,
        completed: _Optional[_Union[AllocationCompleted, _Mapping]] = ...,
        failed: _Optional[_Union[AllocationFailed, _Mapping]] = ...,
    ) -> None: ...

class AllocationLogEntry(_message.Message):
    __slots__ = ("allocation_id", "clock", "call_function", "function_call_result")
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    CLOCK_FIELD_NUMBER: _ClassVar[int]
    CALL_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_RESULT_FIELD_NUMBER: _ClassVar[int]
    allocation_id: str
    clock: int
    call_function: FunctionCallRequest
    function_call_result: FunctionCallResult
    def __init__(
        self,
        allocation_id: _Optional[str] = ...,
        clock: _Optional[int] = ...,
        call_function: _Optional[_Union[FunctionCallRequest, _Mapping]] = ...,
        function_call_result: _Optional[_Union[FunctionCallResult, _Mapping]] = ...,
    ) -> None: ...

class GetAllocationLogRequest(_message.Message):
    __slots__ = ("allocation_id", "after_clock", "max_entries")
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_CLOCK_FIELD_NUMBER: _ClassVar[int]
    MAX_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    allocation_id: str
    after_clock: int
    max_entries: int
    def __init__(
        self,
        allocation_id: _Optional[str] = ...,
        after_clock: _Optional[int] = ...,
        max_entries: _Optional[int] = ...,
    ) -> None: ...

class GetAllocationLogResponse(_message.Message):
    __slots__ = ("entries", "last_clock", "has_more")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    LAST_CLOCK_FIELD_NUMBER: _ClassVar[int]
    HAS_MORE_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[AllocationLogEntry]
    last_clock: int
    has_more: bool
    def __init__(
        self,
        entries: _Optional[_Iterable[_Union[AllocationLogEntry, _Mapping]]] = ...,
        last_clock: _Optional[int] = ...,
        has_more: bool = ...,
    ) -> None: ...

class PollCommandsRequest(_message.Message):
    __slots__ = ("executor_id", "acked_command_seq")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    ACKED_COMMAND_SEQ_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    acked_command_seq: int
    def __init__(
        self, executor_id: _Optional[str] = ..., acked_command_seq: _Optional[int] = ...
    ) -> None: ...

class PollCommandsResponse(_message.Message):
    __slots__ = ("commands",)
    COMMANDS_FIELD_NUMBER: _ClassVar[int]
    commands: _containers.RepeatedCompositeFieldContainer[Command]
    def __init__(
        self, commands: _Optional[_Iterable[_Union[Command, _Mapping]]] = ...
    ) -> None: ...

class PollAllocationResultsRequest(_message.Message):
    __slots__ = ("executor_id", "acked_result_seq")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    ACKED_RESULT_SEQ_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    acked_result_seq: int
    def __init__(
        self, executor_id: _Optional[str] = ..., acked_result_seq: _Optional[int] = ...
    ) -> None: ...

class PollAllocationResultsResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[SequencedAllocationResult]
    def __init__(
        self,
        results: _Optional[
            _Iterable[_Union[SequencedAllocationResult, _Mapping]]
        ] = ...,
    ) -> None: ...

class SequencedAllocationResult(_message.Message):
    __slots__ = ("seq", "entry")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    ENTRY_FIELD_NUMBER: _ClassVar[int]
    seq: int
    entry: AllocationLogEntry
    def __init__(
        self,
        seq: _Optional[int] = ...,
        entry: _Optional[_Union[AllocationLogEntry, _Mapping]] = ...,
    ) -> None: ...
