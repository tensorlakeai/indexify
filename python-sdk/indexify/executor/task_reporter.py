import io
from typing import List, Optional

import httpx
import nanoid
from rich import print

from indexify.executor.api_objects import RouterOutput as ApiRouterOutput
from indexify.executor.api_objects import Task, TaskResult
from indexify.executor.task_store import CompletedTask
from indexify.functions_sdk.data_objects import IndexifyData, RouterOutput
from indexify.functions_sdk.object_serializer import MsgPackSerializer


# https://github.com/psf/requests/issues/1081#issuecomment-428504128
class ForceMultipartDict(dict):
    def __bool__(self):
        return True


FORCE_MULTIPART = ForceMultipartDict()


class TaskReporter:
    def __init__(self, base_url: str, executor_id: str):
        self._base_url = base_url
        self._executor_id = executor_id

    def report_task_outcome(self, completed_task: CompletedTask):
        fn_outputs = []
        print(
            f"[bold]task-reporter[/bold] uploading output of size: {len(completed_task.outputs or [])}"
        )
        for output in completed_task.outputs or []:
            output_bytes = MsgPackSerializer.serialize(output)
            fn_outputs.append(
                ("node_outputs", (nanoid.generate(), io.BytesIO(output_bytes)))
            )

        if completed_task.errors:
            print(
                f"[bold]task-reporter[/bold] uploading error of size: {len(completed_task.errors)}"
            )
            fn_outputs.append(
                (
                    "exception_msg",
                    (nanoid.generate(), io.BytesIO(completed_task.errors.encode())),
                )
            )

        if completed_task.stdout:
            print(
                f"[bold]task-reporter[/bold] uploading stdout of size: {len(completed_task.stdout)}"
            )
            fn_outputs.append(
                (
                    "stdout",
                    (nanoid.generate(), io.BytesIO(completed_task.stdout.encode())),
                )
            )

        if completed_task.stderr:
            print(
                f"[bold]task-reporter[/bold] uploading stderr of size: {len(completed_task.stderr)}"
            )
            fn_outputs.append(
                (
                    "stderr",
                    (nanoid.generate(), io.BytesIO(completed_task.stderr.encode())),
                )
            )

        router_output = (
            ApiRouterOutput(edges=completed_task.router_output.edges)
            if completed_task.router_output
            else None
        )

        task_result = TaskResult(
            router_output=router_output,
            outcome=completed_task.task_outcome,
            namespace=completed_task.task.namespace,
            compute_graph=completed_task.task.compute_graph,
            compute_fn=completed_task.task.compute_fn,
            invocation_id=completed_task.task.invocation_id,
            executor_id=self._executor_id,
            task_id=completed_task.task.id,
            reducer=completed_task.reducer,
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
