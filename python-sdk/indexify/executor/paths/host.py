import os
from os import path


class HostPaths:
    """Paths in host filesystem for task execution.

    Some of the files like graph, function code and function image can be cached after a task is finished
    but this is not curently implemented to simplify the implementation and not require cache eviction.

    There's also a known issue that graphs currently don't have IDs and using graph names in cache keys
    results in potential conflicts when a graph gets invoked then deleted and created again with the same name.
    So we can't implement correct caching right now.
    """

    @classmethod
    def set_base_dir(cls, path: str) -> None:
        """Sets root directory for all task data in host filesystem."""
        cls._base_dir_path = path
        os.makedirs(path, exist_ok=True)

    @classmethod
    def base_dir(cls) -> str:
        """Returns absolute path to root directory for all task data in host filesystem."""
        return cls._base_dir_path

    @classmethod
    def task_dir(cls, task_id: str) -> str:
        """Returns absolute path to root directory for all task data in host filesystem."""
        return path.join(cls._base_dir_path, "tasks", task_id)

    @classmethod
    def task_run_function_request(cls, task_id: str) -> str:
        """Returns absolute path to file with JSON serialized RunFunctionRequest in host filesystem."""
        return path.join(cls.task_dir(task_id), "run_function_request.json")

    @classmethod
    def task_run_function_response(cls, task_id: str) -> str:
        """Returns absolute path to file with JSON serialized RunFunctionResponse in host filesystem."""
        return path.join(cls.task_dir(task_id), "run_function_response.json")
