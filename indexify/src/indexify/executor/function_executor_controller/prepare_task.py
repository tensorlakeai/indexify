from typing import Any

from indexify.executor.blob_store.blob_store import BLOBStore

from .downloads import download_init_value, download_input
from .events import TaskPreparationFinished
from .task_info import TaskInfo


async def prepare_task(
    task_info: TaskInfo, blob_store: BLOBStore, logger: Any
) -> TaskPreparationFinished:
    """Prepares the task by downloading the input and init value if available.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    try:
        task_info.input = await download_input(
            data_payload=task_info.allocation.task.input,
            blob_store=blob_store,
            logger=logger,
        )

        if task_info.allocation.task.HasField("reducer_input"):
            task_info.init_value = await download_init_value(
                data_payload=task_info.allocation.task.reducer_input,
                blob_store=blob_store,
                logger=logger,
            )

        return TaskPreparationFinished(task_info=task_info, is_success=True)
    except Exception as e:
        logger.error(
            "Failed to prepare task",
            exc_info=e,
        )
        return TaskPreparationFinished(task_info=task_info, is_success=False)
