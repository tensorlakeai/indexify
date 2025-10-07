from collections.abc import Iterable

from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.proto.executor_api_pb2 import (
    Allocation,
    DataPayload,
    FunctionExecutorDescription,
    FunctionRef,
)


def validate_function_executor_description(
    function_executor_description: FunctionExecutorDescription,
) -> None:
    """Validates the supplied FE description.

    Raises ValueError if the description is not valid.
    """
    validator = MessageValidator(function_executor_description)
    validator.required_field("id")
    validator.required_field("function")
    _validate_function_ref(function_executor_description.function)
    # secret_names can be empty.
    validator.required_field("initialization_timeout_ms")
    validator.required_field("application")
    validator.required_field("resources")
    validator.required_field("output_payload_uri_prefix")
    validator.required_field("max_concurrency")
    validator.required_field("allocation_timeout_ms")

    _validate_data_payload(function_executor_description.application)

    validator = MessageValidator(function_executor_description.resources)
    validator.required_field("cpu_ms_per_sec")
    validator.required_field("memory_bytes")
    validator.required_field("disk_bytes")

    if function_executor_description.resources.HasField("gpu"):
        validator = MessageValidator(function_executor_description.resources.gpu)
        validator.required_field("count")
        validator.required_field("model")


def validate_allocation(alloc: Allocation) -> None:
    """Validates the supplied TaskAllocation.

    Raises ValueError if the TaskAllocation is not valid.
    """
    validator = MessageValidator(alloc)
    validator.required_field("function")
    _validate_function_ref(alloc.function)
    validator.required_field("allocation_id")
    validator.required_field("function_call_id")
    validator.required_field("request_id")
    _validate_data_payloads(alloc.args, field_name="TaskAllocation.args")
    validator.required_field("output_payload_uri_prefix")
    validator.required_field("request_error_payload_uri_prefix")
    validator.required_field("function_executor_id")
    validator.required_field("function_call_metadata")


def _validate_data_payloads(
    data_payloads: Iterable[DataPayload], field_name: str
) -> None:
    """Validates the supplied iterable of DataPayloads."""
    for data_payload in data_payloads:
        _validate_data_payload(data_payload)


def _validate_data_payload(data_payload: DataPayload) -> None:
    """Validates the supplied DataPayload.

    Raises ValueError if the DataPayload is not valid.
    """
    (
        MessageValidator(data_payload)
        .required_field("uri")
        .required_field("encoding")
        .required_field("encoding_version")
        # content_type is optional.
        # metadata_size is optional.
        .required_field("offset")
        .required_field("size")
        .required_field("sha256_hash")
        # source_function_call_id is not used
        # id is not used
    )


def _validate_function_ref(function_ref: FunctionRef) -> None:
    """Validates the supplied FunctionRef.

    Raises ValueError if the FunctionRef is not valid.
    """
    (
        MessageValidator(function_ref)
        .required_field("namespace")
        .required_field("application_name")
        .required_field("function_name")
        # application_version is not always set by Server.
    )
