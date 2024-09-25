import io
from typing import List, Optional

import httpx
import nanoid
from rich import print

from indexify.executor.api_objects import RouterOutput as ApiRouterOutput
from indexify.executor.api_objects import Task, TaskResult
from indexify.functions_sdk.cbor_serializer import CborSerializer
from indexify.functions_sdk.data_objects import IndexifyData, RouterOutput


# https://github.com/psf/requests/issues/1081#issuecomment-428504128
class ForceMultipartDict(dict):
    def __bool__(self):
        return True


FORCE_MULTIPART = ForceMultipartDict()


class TaskReporter:
    def __init__(self, base_url: str, executor_id: str):
        self._base_url = base_url
        self._executor_id = executor_id

    def report_task_outcome(
        self,
        outputs: List[IndexifyData],
        router_output: Optional[RouterOutput],
        task: Task,
        outcome: str,
        exception_msg: Optional[str],
        stdout: Optional[str],
        stderr: Optional[str],
    ):
        fn_outputs = []
        for output in outputs:
            print(
                f"[bold]task-reporter[/bold] uploading output of size: {len(output.payload)}"
            )
            output_bytes = CborSerializer.serialize(output)
            fn_outputs.append(
                ("node_outputs", (nanoid.generate(), io.BytesIO(output_bytes)))
            )

        if exception_msg:
            print(
                f"[bold]task-reporter[/bold] uploading error of size: {len(exception_msg)}"
            )
            fn_outputs.append(
                (
                    "exception_msg",
                    (nanoid.generate(), io.BytesIO(exception_msg.encode())),
                )
            )

        if stdout:
            print(
                f"[bold]task-reporter[/bold] uploading stdout of size: {len(stdout)}"
            )
            fn_outputs.append(
                ("stdout", (nanoid.generate(), io.BytesIO(stdout.encode())))
            )

        if stderr:
            print(
                f"[bold]task-reporter[/bold] uploading stderr of size: {len(stderr)}"
            )
            fn_outputs.append(
                ("stderr", (nanoid.generate(), io.BytesIO(stderr.encode())))
            )

        router_output = (
            ApiRouterOutput(edges=router_output.edges) if router_output else None
        )

        task_result = TaskResult(
            router_output=router_output,
            outcome=outcome,
            namespace=task.namespace,
            compute_graph=task.compute_graph,
            compute_fn=task.compute_fn,
            invocation_id=task.invocation_id,
            executor_id=self._executor_id,
            task_id=task.id,
        )
        task_result_data = task_result.model_dump_json(exclude_none=True)

        kwargs = {"data": {"task_result": task_result_data}}
        if fn_outputs and len(fn_outputs) > 0:
            kwargs["files"] = fn_outputs
        else:
            kwargs["files"] = FORCE_MULTIPART
        try:
            response = httpx.post(
                url=f"{self._base_url}/internal/ingest_files",
                **kwargs,
            )
        except Exception as e:
            print(f"failed to report task outcome {e}")
            raise e

        try:
            response.raise_for_status()
        except Exception as e:
            print(f"failed to report task outcome {response.text}")
            raise e
