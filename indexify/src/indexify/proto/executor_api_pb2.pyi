from typing import ClassVar as _ClassVar
from typing import Iterable as _Iterable
from typing import Mapping as _Mapping
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

class GPUModel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GPU_MODEL_UNKNOWN: _ClassVar[GPUModel]

class FunctionExecutorStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FUNCTION_EXECUTOR_STATUS_UNKNOWN: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_STARTING_UP: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_CUSTOMER_ERROR: _ClassVar[
        FunctionExecutorStatus
    ]
    FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_PLATFORM_ERROR: _ClassVar[
        FunctionExecutorStatus
    ]
    FUNCTION_EXECUTOR_STATUS_IDLE: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_RUNNING_TASK: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_UNHEALTHY: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_STOPPING: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_STOPPED: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_SHUTDOWN: _ClassVar[FunctionExecutorStatus]

class ExecutorStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXECUTOR_STATUS_UNKNOWN: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_STARTING_UP: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_RUNNING: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_DRAINED: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_STOPPING: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_STOPPED: _ClassVar[ExecutorStatus]

class ExecutorFlavor(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXECUTOR_FLAVOR_UNKNOWN: _ClassVar[ExecutorFlavor]
    EXECUTOR_FLAVOR_OSS: _ClassVar[ExecutorFlavor]
    EXECUTOR_FLAVOR_PLATFORM: _ClassVar[ExecutorFlavor]

class TaskOutcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_OUTCOME_UNKNOWN: _ClassVar[TaskOutcome]
    TASK_OUTCOME_SUCCESS: _ClassVar[TaskOutcome]
    TASK_OUTCOME_FAILURE: _ClassVar[TaskOutcome]

class OutputEncoding(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTPUT_ENCODING_UNKNOWN: _ClassVar[OutputEncoding]
    OUTPUT_ENCODING_JSON: _ClassVar[OutputEncoding]
    OUTPUT_ENCODING_PICKLE: _ClassVar[OutputEncoding]
    OUTPUT_ENCODING_BINARY: _ClassVar[OutputEncoding]

DATA_PAYLOAD_ENCODING_UNKNOWN: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_UTF8_JSON: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_UTF8_TEXT: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_BINARY_PICKLE: DataPayloadEncoding
GPU_MODEL_UNKNOWN: GPUModel
FUNCTION_EXECUTOR_STATUS_UNKNOWN: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STARTING_UP: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_CUSTOMER_ERROR: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_PLATFORM_ERROR: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_IDLE: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_RUNNING_TASK: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_UNHEALTHY: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STOPPING: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STOPPED: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_SHUTDOWN: FunctionExecutorStatus
EXECUTOR_STATUS_UNKNOWN: ExecutorStatus
EXECUTOR_STATUS_STARTING_UP: ExecutorStatus
EXECUTOR_STATUS_RUNNING: ExecutorStatus
EXECUTOR_STATUS_DRAINED: ExecutorStatus
EXECUTOR_STATUS_STOPPING: ExecutorStatus
EXECUTOR_STATUS_STOPPED: ExecutorStatus
EXECUTOR_FLAVOR_UNKNOWN: ExecutorFlavor
EXECUTOR_FLAVOR_OSS: ExecutorFlavor
EXECUTOR_FLAVOR_PLATFORM: ExecutorFlavor
TASK_OUTCOME_UNKNOWN: TaskOutcome
TASK_OUTCOME_SUCCESS: TaskOutcome
TASK_OUTCOME_FAILURE: TaskOutcome
OUTPUT_ENCODING_UNKNOWN: OutputEncoding
OUTPUT_ENCODING_JSON: OutputEncoding
OUTPUT_ENCODING_PICKLE: OutputEncoding
OUTPUT_ENCODING_BINARY: OutputEncoding

class DataPayload(_message.Message):
    __slots__ = ("path", "size", "sha256_hash", "uri", "encoding", "encoding_version")
    PATH_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    SHA256_HASH_FIELD_NUMBER: _ClassVar[int]
    URI_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    ENCODING_VERSION_FIELD_NUMBER: _ClassVar[int]
    path: str
    size: int
    sha256_hash: str
    uri: str
    encoding: DataPayloadEncoding
    encoding_version: int
    def __init__(
        self,
        path: _Optional[str] = ...,
        size: _Optional[int] = ...,
        sha256_hash: _Optional[str] = ...,
        uri: _Optional[str] = ...,
        encoding: _Optional[_Union[DataPayloadEncoding, str]] = ...,
        encoding_version: _Optional[int] = ...,
    ) -> None: ...

class GPUResources(_message.Message):
    __slots__ = ("count", "deprecated_model", "model")
    COUNT_FIELD_NUMBER: _ClassVar[int]
    DEPRECATED_MODEL_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    count: int
    deprecated_model: GPUModel
    model: str
    def __init__(
        self,
        count: _Optional[int] = ...,
        deprecated_model: _Optional[_Union[GPUModel, str]] = ...,
        model: _Optional[str] = ...,
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
    __slots__ = ("namespace", "graph_name", "function_name", "graph_version")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    graph_name: str
    function_name: str
    graph_version: str
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
    ) -> None: ...

class FunctionExecutorResources(_message.Message):
    __slots__ = ("cpu_ms_per_sec", "memory_bytes", "disk_bytes", "gpu_count")
    CPU_MS_PER_SEC_FIELD_NUMBER: _ClassVar[int]
    MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    DISK_BYTES_FIELD_NUMBER: _ClassVar[int]
    GPU_COUNT_FIELD_NUMBER: _ClassVar[int]
    cpu_ms_per_sec: int
    memory_bytes: int
    disk_bytes: int
    gpu_count: int
    def __init__(
        self,
        cpu_ms_per_sec: _Optional[int] = ...,
        memory_bytes: _Optional[int] = ...,
        disk_bytes: _Optional[int] = ...,
        gpu_count: _Optional[int] = ...,
    ) -> None: ...

class FunctionExecutorDescription(_message.Message):
    __slots__ = (
        "id",
        "namespace",
        "graph_name",
        "graph_version",
        "function_name",
        "image_uri",
        "secret_names",
        "resource_limits",
        "customer_code_timeout_ms",
        "graph",
        "resources",
    )
    ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    IMAGE_URI_FIELD_NUMBER: _ClassVar[int]
    SECRET_NAMES_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_LIMITS_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_CODE_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    id: str
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    image_uri: str
    secret_names: _containers.RepeatedScalarFieldContainer[str]
    resource_limits: HostResources
    customer_code_timeout_ms: int
    graph: DataPayload
    resources: FunctionExecutorResources
    def __init__(
        self,
        id: _Optional[str] = ...,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        image_uri: _Optional[str] = ...,
        secret_names: _Optional[_Iterable[str]] = ...,
        resource_limits: _Optional[_Union[HostResources, _Mapping]] = ...,
        customer_code_timeout_ms: _Optional[int] = ...,
        graph: _Optional[_Union[DataPayload, _Mapping]] = ...,
        resources: _Optional[_Union[FunctionExecutorResources, _Mapping]] = ...,
    ) -> None: ...

class FunctionExecutorState(_message.Message):
    __slots__ = ("description", "status", "status_message")
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    STATUS_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    description: FunctionExecutorDescription
    status: FunctionExecutorStatus
    status_message: str
    def __init__(
        self,
        description: _Optional[_Union[FunctionExecutorDescription, _Mapping]] = ...,
        status: _Optional[_Union[FunctionExecutorStatus, str]] = ...,
        status_message: _Optional[str] = ...,
    ) -> None: ...

class ExecutorState(_message.Message):
    __slots__ = (
        "executor_id",
        "development_mode",
        "hostname",
        "flavor",
        "version",
        "status",
        "total_resources",
        "free_resources",
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
    DEVELOPMENT_MODE_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    FLAVOR_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    FREE_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_EXECUTOR_STATES_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    STATE_HASH_FIELD_NUMBER: _ClassVar[int]
    SERVER_CLOCK_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    development_mode: bool
    hostname: str
    flavor: ExecutorFlavor
    version: str
    status: ExecutorStatus
    total_resources: HostResources
    free_resources: HostResources
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
        development_mode: bool = ...,
        hostname: _Optional[str] = ...,
        flavor: _Optional[_Union[ExecutorFlavor, str]] = ...,
        version: _Optional[str] = ...,
        status: _Optional[_Union[ExecutorStatus, str]] = ...,
        total_resources: _Optional[_Union[HostResources, _Mapping]] = ...,
        free_resources: _Optional[_Union[HostResources, _Mapping]] = ...,
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

class ReportExecutorStateRequest(_message.Message):
    __slots__ = ("executor_state",)
    EXECUTOR_STATE_FIELD_NUMBER: _ClassVar[int]
    executor_state: ExecutorState
    def __init__(
        self, executor_state: _Optional[_Union[ExecutorState, _Mapping]] = ...
    ) -> None: ...

class ReportExecutorStateResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TaskRetryPolicy(_message.Message):
    __slots__ = ("max_retries", "initial_delay_ms", "max_delay_ms", "delay_multiplier")
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    INITIAL_DELAY_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_DELAY_MS_FIELD_NUMBER: _ClassVar[int]
    DELAY_MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    max_retries: int
    initial_delay_ms: int
    max_delay_ms: int
    delay_multiplier: int
    def __init__(
        self,
        max_retries: _Optional[int] = ...,
        initial_delay_ms: _Optional[int] = ...,
        max_delay_ms: _Optional[int] = ...,
        delay_multiplier: _Optional[int] = ...,
    ) -> None: ...

class Task(_message.Message):
    __slots__ = (
        "id",
        "namespace",
        "graph_name",
        "graph_version",
        "function_name",
        "graph_invocation_id",
        "input_key",
        "reducer_output_key",
        "timeout_ms",
        "input",
        "reducer_input",
        "output_payload_uri_prefix",
        "retry_policy",
    )
    ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_KEY_FIELD_NUMBER: _ClassVar[int]
    REDUCER_OUTPUT_KEY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    REDUCER_INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PAYLOAD_URI_PREFIX_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    id: str
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph_invocation_id: str
    input_key: str
    reducer_output_key: str
    timeout_ms: int
    input: DataPayload
    reducer_input: DataPayload
    output_payload_uri_prefix: str
    retry_policy: TaskRetryPolicy
    def __init__(
        self,
        id: _Optional[str] = ...,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        graph_invocation_id: _Optional[str] = ...,
        input_key: _Optional[str] = ...,
        reducer_output_key: _Optional[str] = ...,
        timeout_ms: _Optional[int] = ...,
        input: _Optional[_Union[DataPayload, _Mapping]] = ...,
        reducer_input: _Optional[_Union[DataPayload, _Mapping]] = ...,
        output_payload_uri_prefix: _Optional[str] = ...,
        retry_policy: _Optional[_Union[TaskRetryPolicy, _Mapping]] = ...,
    ) -> None: ...

class TaskAllocation(_message.Message):
    __slots__ = ("function_executor_id", "task")
    FUNCTION_EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    function_executor_id: str
    task: Task
    def __init__(
        self,
        function_executor_id: _Optional[str] = ...,
        task: _Optional[_Union[Task, _Mapping]] = ...,
    ) -> None: ...

class GetDesiredExecutorStatesRequest(_message.Message):
    __slots__ = ("executor_id",)
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    def __init__(self, executor_id: _Optional[str] = ...) -> None: ...

class DesiredExecutorState(_message.Message):
    __slots__ = ("function_executors", "task_allocations", "clock")
    FUNCTION_EXECUTORS_FIELD_NUMBER: _ClassVar[int]
    TASK_ALLOCATIONS_FIELD_NUMBER: _ClassVar[int]
    CLOCK_FIELD_NUMBER: _ClassVar[int]
    function_executors: _containers.RepeatedCompositeFieldContainer[
        FunctionExecutorDescription
    ]
    task_allocations: _containers.RepeatedCompositeFieldContainer[TaskAllocation]
    clock: int
    def __init__(
        self,
        function_executors: _Optional[
            _Iterable[_Union[FunctionExecutorDescription, _Mapping]]
        ] = ...,
        task_allocations: _Optional[_Iterable[_Union[TaskAllocation, _Mapping]]] = ...,
        clock: _Optional[int] = ...,
    ) -> None: ...

class ReportTaskOutcomeRequest(_message.Message):
    __slots__ = (
        "task_id",
        "namespace",
        "graph_name",
        "function_name",
        "graph_invocation_id",
        "outcome",
        "invocation_id",
        "executor_id",
        "reducer",
        "next_functions",
        "fn_outputs",
        "stdout",
        "stderr",
        "output_encoding",
        "output_encoding_version",
    )
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    REDUCER_FIELD_NUMBER: _ClassVar[int]
    NEXT_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    FN_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_ENCODING_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_ENCODING_VERSION_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    namespace: str
    graph_name: str
    function_name: str
    graph_invocation_id: str
    outcome: TaskOutcome
    invocation_id: str
    executor_id: str
    reducer: bool
    next_functions: _containers.RepeatedScalarFieldContainer[str]
    fn_outputs: _containers.RepeatedCompositeFieldContainer[DataPayload]
    stdout: DataPayload
    stderr: DataPayload
    output_encoding: OutputEncoding
    output_encoding_version: int
    def __init__(
        self,
        task_id: _Optional[str] = ...,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        graph_invocation_id: _Optional[str] = ...,
        outcome: _Optional[_Union[TaskOutcome, str]] = ...,
        invocation_id: _Optional[str] = ...,
        executor_id: _Optional[str] = ...,
        reducer: bool = ...,
        next_functions: _Optional[_Iterable[str]] = ...,
        fn_outputs: _Optional[_Iterable[_Union[DataPayload, _Mapping]]] = ...,
        stdout: _Optional[_Union[DataPayload, _Mapping]] = ...,
        stderr: _Optional[_Union[DataPayload, _Mapping]] = ...,
        output_encoding: _Optional[_Union[OutputEncoding, str]] = ...,
        output_encoding_version: _Optional[int] = ...,
    ) -> None: ...

class ReportTaskOutcomeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
