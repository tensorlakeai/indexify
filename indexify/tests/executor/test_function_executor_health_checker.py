import asyncio
import multiprocessing
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

import grpc
import structlog
from tensorlake.function_executor.proto.function_executor_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
    RunTaskRequest,
    RunTaskResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorServicer,
    FunctionExecutorStub,
    add_FunctionExecutorServicer_to_server,
)

from indexify.executor.function_executor.health_checker import (
    HealthChecker,
    HealthCheckResult,
)


def run_task_busyloop(request: RunTaskRequest) -> RunTaskResponse:
    # Emulates a CPU intensive task that doesn't release Python GIL.
    while True:
        pass


class FunctionExecutorTestService(FunctionExecutorServicer):
    def __init__(
        self,
        run_task_func: Callable[[RunTaskRequest], RunTaskResponse],
        run_task_started_event: multiprocessing.Event,
    ):
        self._run_task_func = run_task_func
        self._run_task_started_event = run_task_started_event

    def run_task(
        self, request: RunTaskRequest, context: grpc.ServicerContext
    ) -> RunTaskResponse:
        self._run_task_started_event.set()
        return self._run_task_func(request)

    def check_health(
        self, request: HealthCheckRequest, context: grpc.ServicerContext
    ) -> HealthCheckResponse:
        return HealthCheckResponse(healthy=True, status_message="ok")


def run_function_executor_server(
    server_address: str,
    run_task_func: Callable[[RunTaskRequest], RunTaskResponse],
    max_workers: int,
    run_task_started_event: multiprocessing.Event,
):
    function_executor_service = FunctionExecutorTestService(
        run_task_func, run_task_started_event
    )
    server: grpc.Server = grpc.server(
        thread_pool=ThreadPoolExecutor(max_workers=max_workers),
    )
    add_FunctionExecutorServicer_to_server(function_executor_service, server)
    server.add_insecure_port(server_address)
    server.start()
    server.wait_for_termination()


async def run_task_rpc(stub: FunctionExecutorStub):
    await stub.run_task(RunTaskRequest())


def wait_event(event: multiprocessing.Event) -> None:
    event.wait()


class ProcessContextManager:
    def __init__(self, process: multiprocessing.Process):
        self.process = process

    def __enter__(self) -> multiprocessing.Process:
        self.process.start()
        return self.process

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.process.is_alive():
            self.process.kill()
            self.process.join()


class TaskContextManager:
    def __init__(self, task: asyncio.Task):
        self._task = task

    async def __aenter__(self):
        return self._task

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass


class TestHealthChecker(unittest.IsolatedAsyncioTestCase):
    async def test_function_executor_health_check_succeeds_when_running_cpu_intensive_function(
        self,
    ):
        run_task_busyloop_started = multiprocessing.Event()
        # It's okay to fork without exec before we start using grpc in the parent process.
        with ProcessContextManager(
            multiprocessing.Process(
                target=run_function_executor_server,
                kwargs={
                    "server_address": "localhost:60000",
                    "run_task_func": run_task_busyloop,
                    "max_workers": 1,  # Don't allow other threads to process health checks while run task RPC is busylooping.
                    "run_task_started_event": run_task_busyloop_started,
                },
            )
        ):
            channel = grpc.aio.insecure_channel("localhost:60000")
            await asyncio.wait_for(
                channel.channel_ready(),
                timeout=10,
            )
            stub = FunctionExecutorStub(channel)
            async with TaskContextManager(asyncio.create_task(run_task_rpc(stub))):
                await asyncio.to_thread(wait_event, run_task_busyloop_started)

                fe_health_checker = HealthChecker(channel, stub, structlog.get_logger())
                result: HealthCheckResult = await fe_health_checker.check()
                self.assertTrue(result.is_healthy)
                self.assertEqual(
                    result.reason,
                    "Health check RPC failed with status code: DEADLINE_EXCEEDED. Assuming Function Executor is healthy.",
                )

    async def test_function_executor_health_check_fails_after_function_executor_got_killed(
        self,
    ):
        run_task_busyloop_started = multiprocessing.Event()
        # It's okay to fork without exec before we start using grpc in the parent process.
        with ProcessContextManager(
            multiprocessing.Process(
                target=run_function_executor_server,
                kwargs={
                    "server_address": "localhost:60001",
                    "run_task_func": run_task_busyloop,
                    "max_workers": 100,  # Don't allow other threads to process health checks while run task RPC is busylooping.
                    "run_task_started_event": run_task_busyloop_started,
                },
            )
        ) as fe_process:
            channel = grpc.aio.insecure_channel("localhost:60001")
            await asyncio.wait_for(
                channel.channel_ready(),
                timeout=10,
            )
            stub = FunctionExecutorStub(channel)
            fe_health_checker = HealthChecker(channel, stub, structlog.get_logger())
            result: HealthCheckResult = await fe_health_checker.check()
            self.assertTrue(result.is_healthy)

        fe_process.join()
        result: HealthCheckResult = await fe_health_checker.check()
        self.assertFalse(result.is_healthy)
        self.assertEqual(
            result.reason,
            "Channel is in TRANSIENT_FAILURE state, assuming Function Executor crashed.",
        )

    async def test_function_executor_health_check_succeeds_with_env_var_override(
        self,
    ):
        # We don't run the function executor server in this test so without override
        # there's going to be an exception raised by health checker.
        os.environ["INDEXIFY_DISABLE_FUNCTION_EXECUTOR_HEALTH_CHECKS"] = "1"
        try:
            fe_health_checker = HealthChecker(None, None, structlog.get_logger())
            result: HealthCheckResult = await fe_health_checker.check()
            self.assertTrue(result.is_healthy)
            self.assertEqual(
                result.reason,
                "Function Executor health checks are disabled using INDEXIFY_DISABLE_FUNCTION_EXECUTOR_HEALTH_CHECKS env var.",
            )
        finally:
            del os.environ["INDEXIFY_DISABLE_FUNCTION_EXECUTOR_HEALTH_CHECKS"]


if __name__ == "__main__":
    unittest.main()
