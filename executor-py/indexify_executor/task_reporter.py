import json
from typing import List

import httpx
from indexify.functions_sdk.data_objects import BaseData

from indexify_executor.api_objects import Task, TaskOutput, TaskResult


class TaskReporter:
    def __init__(self, base_url: str, executor_id: str):
        self._base_url = base_url
        self._executor_id = executor_id

    def report_task_outcome(self, outputs: List[BaseData], task: Task, outcome: str):
        api_outputs = []
        for output in outputs:
            api_outputs.append(TaskOutput.to_api_object(output))
        task_result = TaskResult(
            outputs=api_outputs,
            outcome=outcome,
            namespace=task.namespace,
            compute_graph=task.compute_graph,
            compute_fn=task.compute_fn,
            invocation_id=task.invocation_id,
            executor_id=self._executor_id,
            task_id=task.id,
        )
        data = task_result.model_dump(exclude_none=True)
        response = httpx.post(
            f"{self._base_url}/internal/ingest_objects",
            headers={"Content-Type": "application/json"},
            data=json.dumps(data),
        )
        print(response.text)
        response.raise_for_status()
