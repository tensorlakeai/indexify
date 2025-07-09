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

class TaskOutcomeCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_OUTCOME_CODE_UNKNOWN: _ClassVar[TaskOutcomeCode]
    TASK_OUTCOME_CODE_SUCCESS: _ClassVar[TaskOutcomeCode]
    TASK_OUTCOME_CODE_FAILURE: _ClassVar[TaskOutcomeCode]

class TaskFailureReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_FAILURE_REASON_UNKNOWN: _ClassVar[TaskFailureReason]
    TASK_FAILURE_REASON_INTERNAL_ERROR: _ClassVar[TaskFailureReason]
    TASK_FAILURE_REASON_FUNCTION_ERROR: _ClassVar[TaskFailureReason]
    TASK_FAILURE_REASON_FUNCTION_TIMEOUT: _ClassVar[TaskFailureReason]
    TASK_FAILURE_REASON_INVOCATION_ERROR: _ClassVar[TaskFailureReason]
    TASK_FAILURE_REASON_TASK_CANCELLED: _ClassVar[TaskFailureReason]
    TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED: _ClassVar[TaskFailureReason]

DATA_PAYLOAD_ENCODING_UNKNOWN: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_UTF8_JSON: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_UTF8_TEXT: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_BINARY_PICKLE: DataPayloadEncoding
DATA_PAYLOAD_ENCODING_BINARY_ZIP: DataPayloadEncoding
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
TASK_OUTCOME_CODE_UNKNOWN: TaskOutcomeCode
TASK_OUTCOME_CODE_SUCCESS: TaskOutcomeCode
TASK_OUTCOME_CODE_FAILURE: TaskOutcomeCode
TASK_FAILURE_REASON_UNKNOWN: TaskFailureReason
TASK_FAILURE_REASON_INTERNAL_ERROR: TaskFailureReason
TASK_FAILURE_REASON_FUNCTION_ERROR: TaskFailureReason
TASK_FAILURE_REASON_FUNCTION_TIMEOUT: TaskFailureReason
TASK_FAILURE_REASON_INVOCATION_ERROR: TaskFailureReason
TASK_FAILURE_REASON_TASK_CANCELLED: TaskFailureReason
TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED: TaskFailureReason

class DataPayload(_message.Message):
    __slots__ = ("size", "sha256_hash", "uri", "encoding", "encoding_version")
    SIZE_FIELD_NUMBER: _ClassVar[int]
    SHA256_HASH_FIELD_NUMBER: _ClassVar[int]
    URI_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    ENCODING_VERSION_FIELD_NUMBER: _ClassVar[int]
    size: int
    sha256_hash: str
    uri: str
    encoding: DataPayloadEncoding
    encoding_version: int
    def __init__(
        self,
        size: _Optional[int] = ...,
        sha256_hash: _Optional[str] = ...,
        uri: _Optional[str] = ...,
        encoding: _Optional[_Union[DataPayloadEncoding, str]] = ...,
        encoding_version: _Optional[int] = ...,
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

class FunctionExecutorDescription(_message.Message):
    __slots__ = (
        "id",
        "namespace",
        "graph_name",
        "graph_version",
        "function_name",
        "image_uri",
        "secret_names",
        "customer_code_timeout_ms",
        "graph",
        "resources",
        "output_payload_uri_prefix",
    )
    ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    IMAGE_URI_FIELD_NUMBER: _ClassVar[int]
    SECRET_NAMES_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_CODE_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PAYLOAD_URI_PREFIX_FIELD_NUMBER: _ClassVar[int]
    id: str
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    image_uri: str
    secret_names: _containers.RepeatedScalarFieldContainer[str]
    customer_code_timeout_ms: int
    graph: DataPayload
    resources: FunctionExecutorResources
    output_payload_uri_prefix: str
    def __init__(
        self,
        id: _Optional[str] = ...,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        image_uri: _Optional[str] = ...,
        secret_names: _Optional[_Iterable[str]] = ...,
        customer_code_timeout_ms: _Optional[int] = ...,
        graph: _Optional[_Union[DataPayload, _Mapping]] = ...,
        resources: _Optional[_Union[FunctionExecutorResources, _Mapping]] = ...,
        output_payload_uri_prefix: _Optional[str] = ...,
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

class FunctionExecutorUpdate(_message.Message):
    __slots__ = ("description", "startup_stdout", "startup_stderr")
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    STARTUP_STDOUT_FIELD_NUMBER: _ClassVar[int]
    STARTUP_STDERR_FIELD_NUMBER: _ClassVar[int]
    description: FunctionExecutorDescription
    startup_stdout: DataPayload
    startup_stderr: DataPayload
    def __init__(
        self,
        description: _Optional[_Union[FunctionExecutorDescription, _Mapping]] = ...,
        startup_stdout: _Optional[_Union[DataPayload, _Mapping]] = ...,
        startup_stderr: _Optional[_Union[DataPayload, _Mapping]] = ...,
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
    __slots__ = ("executor_id", "task_results", "function_executor_updates")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_RESULTS_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_EXECUTOR_UPDATES_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    task_results: _containers.RepeatedCompositeFieldContainer[TaskResult]
    function_executor_updates: _containers.RepeatedCompositeFieldContainer[
        FunctionExecutorUpdate
    ]
    def __init__(
        self,
        executor_id: _Optional[str] = ...,
        task_results: _Optional[_Iterable[_Union[TaskResult, _Mapping]]] = ...,
        function_executor_updates: _Optional[
            _Iterable[_Union[FunctionExecutorUpdate, _Mapping]]
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
        timeout_ms: _Optional[int] = ...,
        input: _Optional[_Union[DataPayload, _Mapping]] = ...,
        reducer_input: _Optional[_Union[DataPayload, _Mapping]] = ...,
        output_payload_uri_prefix: _Optional[str] = ...,
        retry_policy: _Optional[_Union[TaskRetryPolicy, _Mapping]] = ...,
    ) -> None: ...

class TaskAllocation(_message.Message):
    __slots__ = ("function_executor_id", "task", "allocation_id")
    FUNCTION_EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    function_executor_id: str
    task: Task
    allocation_id: str
    def __init__(
        self,
        function_executor_id: _Optional[str] = ...,
        task: _Optional[_Union[Task, _Mapping]] = ...,
        allocation_id: _Optional[str] = ...,
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

class TaskResult(_message.Message):
    __slots__ = (
        "task_id",
        "allocation_id",
        "namespace",
        "graph_name",
        "graph_version",
        "function_name",
        "graph_invocation_id",
        "outcome_code",
        "failure_reason",
        "next_functions",
        "function_outputs",
        "stdout",
        "stderr",
        "invocation_error_output",
    )
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    NEXT_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ERROR_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    allocation_id: str
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph_invocation_id: str
    outcome_code: TaskOutcomeCode
    failure_reason: TaskFailureReason
    next_functions: _containers.RepeatedScalarFieldContainer[str]
    function_outputs: _containers.RepeatedCompositeFieldContainer[DataPayload]
    stdout: DataPayload
    stderr: DataPayload
    invocation_error_output: DataPayload
    def __init__(
        self,
        task_id: _Optional[str] = ...,
        allocation_id: _Optional[str] = ...,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        graph_invocation_id: _Optional[str] = ...,
        outcome_code: _Optional[_Union[TaskOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[TaskFailureReason, str]] = ...,
        next_functions: _Optional[_Iterable[str]] = ...,
        function_outputs: _Optional[_Iterable[_Union[DataPayload, _Mapping]]] = ...,
        stdout: _Optional[_Union[DataPayload, _Mapping]] = ...,
        stderr: _Optional[_Union[DataPayload, _Mapping]] = ...,
        invocation_error_output: _Optional[_Union[DataPayload, _Mapping]] = ...,
    ) -> None: ...
