import asyncio
from typing import List, Optional

import nanoid

from .agent import ExtractorAgent
from .function_worker import FunctionWorker


def join(
    workers: int,
    server_addr: str = "localhost:8900",
    config_path: Optional[str] = None,
):
    print(f"receiving tasks from server addr: {server_addr}")
    id = nanoid.generate()
    print(f"executor id: {id}")

    function_worker = FunctionWorker(workers=workers)

    agent = ExtractorAgent(
        id,
        num_workers=workers,
        function_worker=function_worker,
        server_addr=server_addr,
        config_path=config_path,
    )

    try:
        asyncio.get_event_loop().run_until_complete(agent.run())
    except asyncio.CancelledError as ex:
        print("exiting gracefully", ex)
