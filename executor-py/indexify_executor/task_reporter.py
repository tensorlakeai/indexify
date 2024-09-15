from typing import List
import io

import httpx
from indexify.functions_sdk.data_objects import BaseData
from indexify.functions_sdk.cbor_serializer import CborSerializer
import nanoid

from indexify_executor.api_objects import Task, TaskOutput, TaskResult

# https://github.com/psf/requests/issues/1081#issuecomment-428504128
class ForceMultipartDict(dict):
    def __bool__(self):
        return True


FORCE_MULTIPART = ForceMultipartDict()  

class TaskReporter:
    def __init__(self, base_url: str, executor_id: str):
        self._base_url = base_url
        self._executor_id = executor_id

    def report_task_outcome(self, outputs: List[bytes], task: Task, outcome: str):
        fn_outputs = []
        for output in outputs:
            fn_outputs.append(("node_outputs", (nanoid.generate(), io.BytesIO(output))))

        task_result = TaskResult(
            router_outputs=[],
            outcome=outcome,
            namespace=task.namespace,
            compute_graph=task.compute_graph,
            compute_fn=task.compute_fn,
            invocation_id=task.invocation_id,
            executor_id=self._executor_id,
            task_id=task.id,
        )
        task_result_data= task_result.model_dump_json(exclude_none=True)
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
