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

class GPUModel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GPU_MODEL_UNKNOWN: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_TESLA_T4_16GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_TESLA_V100_16GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_A10_24GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_A6000_48GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_A100_SXM4_40GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_A100_SXM4_80GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_A100_PCI_40GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_H100_SXM5_80GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_H100_PCI_80GB: _ClassVar[GPUModel]
    GPU_MODEL_NVIDIA_RTX_6000_24GB: _ClassVar[GPUModel]

class FunctionExecutorStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FUNCTION_EXECUTOR_STATUS_UNKNOWN: _ClassVar[FunctionExecutorStatus]
    FUNCTION_EXECUTOR_STATUS_STOPPED: _ClassVar[FunctionExecutorStatus]
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

class ExecutorStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXECUTOR_STATUS_UNKNOWN: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_STARTING: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_RUNNING: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_DRAINED: _ClassVar[ExecutorStatus]
    EXECUTOR_STATUS_SHUTTING_DOWN: _ClassVar[ExecutorStatus]

GPU_MODEL_UNKNOWN: GPUModel
GPU_MODEL_NVIDIA_TESLA_T4_16GB: GPUModel
GPU_MODEL_NVIDIA_TESLA_V100_16GB: GPUModel
GPU_MODEL_NVIDIA_A10_24GB: GPUModel
GPU_MODEL_NVIDIA_A6000_48GB: GPUModel
GPU_MODEL_NVIDIA_A100_SXM4_40GB: GPUModel
GPU_MODEL_NVIDIA_A100_SXM4_80GB: GPUModel
GPU_MODEL_NVIDIA_A100_PCI_40GB: GPUModel
GPU_MODEL_NVIDIA_H100_SXM5_80GB: GPUModel
GPU_MODEL_NVIDIA_H100_PCI_80GB: GPUModel
GPU_MODEL_NVIDIA_RTX_6000_24GB: GPUModel
FUNCTION_EXECUTOR_STATUS_UNKNOWN: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STOPPED: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STARTING_UP: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_CUSTOMER_ERROR: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_PLATFORM_ERROR: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_IDLE: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_RUNNING_TASK: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_UNHEALTHY: FunctionExecutorStatus
FUNCTION_EXECUTOR_STATUS_STOPPING: FunctionExecutorStatus
EXECUTOR_STATUS_UNKNOWN: ExecutorStatus
EXECUTOR_STATUS_STARTING: ExecutorStatus
EXECUTOR_STATUS_RUNNING: ExecutorStatus
EXECUTOR_STATUS_DRAINED: ExecutorStatus
EXECUTOR_STATUS_SHUTTING_DOWN: ExecutorStatus

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

class FunctionExecutorDescription(_message.Message):
    __slots__ = (
        "id",
        "namespace",
        "graph_name",
        "graph_version",
        "function_name",
        "image_uri",
    )
    ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    IMAGE_URI_FIELD_NUMBER: _ClassVar[int]
    id: str
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    image_uri: str
    def __init__(
        self,
        id: _Optional[str] = ...,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        image_uri: _Optional[str] = ...,
    ) -> None: ...

class FunctionExecutorState(_message.Message):
    __slots__ = ("description", "status")
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    description: FunctionExecutorDescription
    status: FunctionExecutorStatus
    def __init__(
        self,
        description: _Optional[_Union[FunctionExecutorDescription, _Mapping]] = ...,
        status: _Optional[_Union[FunctionExecutorStatus, str]] = ...,
    ) -> None: ...

class ExecutorState(_message.Message):
    __slots__ = (
        "executor_id",
        "executor_status",
        "host_resources",
        "allowed_functions",
        "function_executor_states",
    )
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_STATUS_FIELD_NUMBER: _ClassVar[int]
    HOST_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_EXECUTOR_STATES_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    executor_status: ExecutorStatus
    host_resources: HostResources
    allowed_functions: _containers.RepeatedCompositeFieldContainer[AllowedFunction]
    function_executor_states: _containers.RepeatedCompositeFieldContainer[
        FunctionExecutorState
    ]
    def __init__(
        self,
        executor_id: _Optional[str] = ...,
        executor_status: _Optional[_Union[ExecutorStatus, str]] = ...,
        host_resources: _Optional[_Union[HostResources, _Mapping]] = ...,
        allowed_functions: _Optional[
            _Iterable[_Union[AllowedFunction, _Mapping]]
        ] = ...,
        function_executor_states: _Optional[
            _Iterable[_Union[FunctionExecutorState, _Mapping]]
        ] = ...,
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
    )
    ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_KEY_FIELD_NUMBER: _ClassVar[int]
    REDUCER_OUTPUT_KEY_FIELD_NUMBER: _ClassVar[int]
    id: str
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph_invocation_id: str
    input_key: str
    reducer_output_key: str
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
