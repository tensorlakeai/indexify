from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.proto.executor_api_pb2 import (
    DataPayload,
    FunctionExecutorDescription,
    TaskAllocation,
)


def validate_function_executor_description(
    function_executor_description: FunctionExecutorDescription,
) -> None:
    """Validates the supplied FE description.

    Raises ValueError if the description is not valid.
    """
    validator = MessageValidator(function_executor_description)
    validator.required_field("id")
    validator.required_field("namespace")
    validator.required_field("graph_name")
    validator.required_field("graph_version")
    validator.required_field("function_name")
    # secret_names can be empty.
    validator.required_field("customer_code_timeout_ms")
    validator.required_field("graph")
    validator.required_field("resources")
    validator.required_field("max_concurrency")

    _validate_data_payload(function_executor_description.graph)

    validator = MessageValidator(function_executor_description.resources)
    validator.required_field("cpu_ms_per_sec")
    validator.required_field("memory_bytes")
    validator.required_field("disk_bytes")

    if function_executor_description.resources.HasField("gpu"):
        validator = MessageValidator(function_executor_description.resources.gpu)
        validator.required_field("count")
        validator.required_field("model")


def validate_task_allocation(task_allocation: TaskAllocation) -> None:
    """Validates the supplied TaskAllocation.

    Raises ValueError if the TaskAllocation is not valid.
    """
    validator = MessageValidator(task_allocation)
    validator.required_field("function_executor_id")
    validator.required_field("allocation_id")
    if not task_allocation.HasField("task"):
        raise ValueError("TaskAllocation must have a 'task' field.")

    validator = MessageValidator(task_allocation.task)
    validator.required_field("id")
    validator.required_field("namespace")
    validator.required_field("graph_name")
    validator.required_field("graph_version")
    validator.required_field("function_name")
    validator.required_field("graph_invocation_id")
    validator.required_field("timeout_ms")
    validator.required_field("input")
    validator.required_field("output_payload_uri_prefix")
    validator.required_field("retry_policy")

    _validate_data_payload(task_allocation.task.input)
    if task_allocation.task.HasField("reducer_input"):
        _validate_data_payload(task_allocation.task.reducer_input)


def _validate_data_payload(data_payload: DataPayload) -> None:
    """Validates the supplied DataPayload.

    Raises ValueError if the DataPayload is not valid.
    """
    (
        MessageValidator(data_payload)
        .required_field("size")
        .required_field("sha256_hash")
        .required_field("uri")
        .required_field("encoding")
        # Ignored by Server right now and not set.
        # .required_field("encoding_version")
        .required_field("offset")
    )
