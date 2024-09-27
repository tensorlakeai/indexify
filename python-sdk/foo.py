import asyncio
import concurrent.futures
import io
import sys
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout


class AsyncLoggingProcessPoolExecutor:
    def __init__(self, max_workers=None):
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)
        self.submit_count = 0
        self.loop = asyncio.get_running_loop()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown(wait=True)

    @staticmethod
    def wrapped_fn(task_id, fn, args, kwargs):
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        print(f"Task {task_id}: Starting execution", file=sys.stdout)
        print(f"Task {task_id}: Starting execution", file=sys.stderr)

        try:
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                result = fn(*args, **kwargs)

            print(f"Task {task_id}: Finished execution", file=sys.stdout)
            print(f"Task {task_id}: Finished execution", file=sys.stderr)

            return result, stdout_capture.getvalue(), stderr_capture.getvalue(), None
        except Exception as e:
            tb = traceback.format_exc()
            print(f"Task {task_id}: Exception occurred", file=sys.stdout)
            print(f"Task {task_id}: Exception occurred", file=sys.stderr)
            print(f"Exception: {str(e)}", file=stdout_capture)
            print(f"Traceback:\n{tb}", file=stdout_capture)
            print(f"Exception: {str(e)}", file=stderr_capture)
            print(f"Traceback:\n{tb}", file=stderr_capture)
            return (
                None,
                stdout_capture.getvalue(),
                stderr_capture.getvalue(),
                (str(e), tb),
            )

    async def submit(self, fn, *args, **kwargs):
        self.submit_count += 1
        task_id = self.submit_count

        future = await self.loop.run_in_executor(
            None, self.executor.submit, self.wrapped_fn, task_id, fn, args, kwargs
        )

        async def callback(future):
            (
                result,
                stdout_log,
                stderr_log,
                exception_info,
            ) = await self.loop.run_in_executor(None, future.result)
            print(f"Task {task_id} stdout:")
            print(stdout_log)
            print(f"Task {task_id} stderr:")
            print(stderr_log)
            if exception_info:
                print(f"Task {task_id} encountered an exception:")
                print(f"Exception: {exception_info[0]}")
                print(f"Traceback:\n{exception_info[1]}")
            else:
                print(f"Task {task_id} result: {result}")
            print(f"Task {task_id} logs truncated.")
            return result

        future.add_done_callback(lambda f: asyncio.create_task(callback(f)))
        return future


# Example usage
def worker_function(x):
    print(f"Processing {x}")
    if x % 2 == 0:
        print(f"Even number: {x}", file=sys.stderr)
    if x == 3:
        raise ValueError("Example exception for x == 3")
    time.sleep(1)  # Simulate some work
    return x * 2


async def main():
    async with AsyncLoggingProcessPoolExecutor(max_workers=2) as executor:
        futures = [await executor.submit(worker_function, i) for i in range(5)]
        results = await asyncio.gather(
            *[asyncio.wrap_future(f) for f in futures], return_exceptions=True
        )
        print("All tasks completed.")
        print("Results:", results)


if __name__ == "__main__":
    asyncio.run(main())
