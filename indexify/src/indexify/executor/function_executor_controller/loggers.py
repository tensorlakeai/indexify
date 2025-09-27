from typing import Any

from indexify.proto.executor_api_pb2 import (
    AllocationResult,
    FunctionExecutorDescription,
    TaskAllocation,
)


def function_executor_logger(
    function_executor_description: FunctionExecutorDescription, logger: Any
) -> Any:
    """Returns a logger bound with the FE's metadata.

    The function assumes that the FE might be invalid."""
    return logger.bind(
        fn_executor_id=(
            function_executor_description.id
            if function_executor_description.HasField("id")
            else None
        ),
        namespace=(
            function_executor_description.function.namespace
            if function_executor_description.HasField("function")
            and function_executor_description.function.HasField("namespace")
            else None
        ),
        # Keep legacy `graph` names in log tags until Server logs fully migrate to `application`.
        graph=(
            function_executor_description.function.application_name
            if function_executor_description.HasField("function")
            and function_executor_description.function.HasField("application_name")
            else None
        ),
        app=(
            function_executor_description.function.application_name
            if function_executor_description.HasField("function")
            and function_executor_description.function.HasField("application_name")
            else None
        ),
        # Keep legacy `graph` names in log tags until Server logs fully migrate to `application`.
        graph_version=(
            function_executor_description.function.application_version
            if function_executor_description.HasField("function")
            and function_executor_description.function.HasField("application_version")
            else None
        ),
        app_version=(
            function_executor_description.function.application_version
            if function_executor_description.HasField("function")
            and function_executor_description.function.HasField("application_version")
            else None
        ),
        fn=(
            function_executor_description.function.function_name
            if function_executor_description.HasField("function")
            and function_executor_description.function.HasField("function_name")
            else None
        ),
    )


def task_allocation_logger(alloc: TaskAllocation, logger: Any) -> Any:
    """Returns a logger for the given TaskAllocation.

    Doesn't assume that the supplied TaskAllocation is valid.
    """
    return logger.bind(
        allocation_id=(
            alloc.allocation_id if alloc.HasField("allocation_id") else None
        ),
        fn_executor_id=(
            alloc.function_executor_id
            if alloc.HasField("function_executor_id")
            else None
        ),
        task_id=alloc.task_id if alloc.HasField("task_id") else None,
        namespace=(
            alloc.function.namespace
            if alloc.HasField("function") and alloc.function.HasField("namespace")
            else None
        ),
        # Keep legacy `graph` names in log tags until Server logs fully migrate to `application`.
        graph=(
            alloc.function.application_name
            if alloc.HasField("function")
            and alloc.function.HasField("application_name")
            else None
        ),
        graph_version=(
            alloc.function.application_version
            if alloc.HasField("function")
            and alloc.function.HasField("application_version")
            else None
        ),
        app=(
            alloc.function.application_name
            if alloc.HasField("function")
            and alloc.function.HasField("application_name")
            else None
        ),
        app_version=(
            alloc.function.application_version
            if alloc.HasField("function")
            and alloc.function.HasField("application_version")
            else None
        ),
        fn=(
            alloc.function.function_name
            if alloc.HasField("function") and alloc.function.HasField("function_name")
            else None
        ),
        # Keep legacy `invocation` names in log tags until Server logs fully migrate to `request`.
        invocation_id=(alloc.request_id if alloc.HasField("request_id") else None),
        request_id=(alloc.request_id if alloc.HasField("request_id") else None),
    )


def allocation_result_logger(alloc_result: AllocationResult, logger: Any) -> Any:
    """Returns a logger bound with the alloc result's metadata.

    The function assumes that the alloc result might be invalid."""
    return logger.bind(
        task_id=alloc_result.task_id if alloc_result.HasField("task_id") else None,
        allocation_id=(
            alloc_result.allocation_id
            if alloc_result.HasField("allocation_id")
            else None
        ),
        namespace=(
            alloc_result.function.namespace
            if alloc_result.HasField("function")
            and alloc_result.function.HasField("namespace")
            else None
        ),
        # Keep legacy `graph` names in log tags until Server logs fully migrate to `application`.
        graph=(
            alloc_result.function.application_name
            if alloc_result.HasField("function")
            and alloc_result.function.HasField("application_name")
            else None
        ),
        graph_version=(
            alloc_result.function.application_version
            if alloc_result.HasField("function")
            and alloc_result.function.HasField("application_version")
            else None
        ),
        app=(
            alloc_result.function.application_name
            if alloc_result.HasField("function")
            and alloc_result.function.HasField("application_name")
            else None
        ),
        app_version=(
            alloc_result.function.application_version
            if alloc_result.HasField("function")
            and alloc_result.function.HasField("application_version")
            else None
        ),
        fn=(
            alloc_result.function.function_name
            if alloc_result.HasField("function")
            and alloc_result.function.HasField("function_name")
            else None
        ),
        # Keep legacy `invocation` names in log tags until Server logs fully migrate to `request`.
        invocation_id=(
            alloc_result.request_id if alloc_result.HasField("request_id") else None
        ),
        request_id=(
            alloc_result.request_id if alloc_result.HasField("request_id") else None
        ),
    )
