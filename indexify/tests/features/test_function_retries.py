import os
import threading
import time
import unittest

from tensorlake.applications import (
    Request,
    RequestFailureException,
    Retries,
    application,
    cls,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications

call_number = 0


@application()
@function(retries=Retries(max_retries=3))
def function_succeeds_after_two_retries(x: int) -> str:
    global call_number
    call_number += 1

    if call_number == 4:
        return "success"
    else:
        raise Exception("Function failed, please retry")


@application()
@function(retries=Retries(max_retries=3))
def function_always_fails(x: int) -> str:
    raise Exception("Function failed and will never succeed")


FUNCTION_ALWAYS_TIMES_OUT_FILE_PATH = "/tmp/function_always_times_out_counter"


@application()
@function(retries=Retries(max_retries=3), timeout=1)
def function_always_times_out(x: int) -> str:
    with open(FUNCTION_ALWAYS_TIMES_OUT_FILE_PATH, "a") as f:
        f.write("executed\n")
    time.sleep(1000)


class TestFunctionRetries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_function_succeeds_after_two_retries(self):
        request: Request = run_remote_application(
            function_succeeds_after_two_retries, 1
        )
        self.assertEqual(request.output(), "success")

    def test_function_fails_after_exhausting_failure_retries(self):
        request: Request = run_remote_application(function_always_fails, 1)
        self.assertRaises(RequestFailureException, request.output)

    def test_function_fails_after_exhausting_timeout_retries(self):
        if os.path.exists(FUNCTION_ALWAYS_TIMES_OUT_FILE_PATH):
            os.remove(FUNCTION_ALWAYS_TIMES_OUT_FILE_PATH)

        request: Request = run_remote_application(function_always_times_out, 1)
        self.assertRaises(RequestFailureException, request.output)

        with open(FUNCTION_ALWAYS_TIMES_OUT_FILE_PATH, "r") as f:
            lines = f.readlines()
            self.assertEqual(lines, ["executed\n"] * 4)  # 3 retries + initial call


@cls()
class FunctionWithFailingConstructor:
    FILE_PATH = "/tmp/FunctionWithFailingConstructor_fail"
    MAX_RETRIES = 3

    def __init__(self):
        super().__init__()
        if os.path.exists(self.FILE_PATH):
            raise Exception("Constructor failed")

    @application()
    @function(retries=Retries(max_retries=MAX_RETRIES))
    def run(self, x: int) -> str:
        return "success"

    @classmethod
    def unfail_constructor(cls):
        os.remove(cls.FILE_PATH)

    @classmethod
    def fail_constructor(cls):
        with open(cls.FILE_PATH, "w") as f:
            f.write(
                "This file is used to fail the constructor of FunctionWithFailingConstructor."
            )


@cls(init_timeout=1)
class FunctionWithTimingOutConstructor:
    FILE_PATH = "/tmp/FunctionWithTimingOutConstructor_timeout"
    MAX_RETRIES = 3

    def __init__(self):
        super().__init__()
        if os.path.exists(self.FILE_PATH):
            time.sleep(1000)

    @application()
    @function(retries=Retries(max_retries=MAX_RETRIES))
    def run(self, x: int) -> str:
        return "success"

    @classmethod
    def untimeout_constructor(cls):
        os.remove(cls.FILE_PATH)

    @classmethod
    def timeout_constructor(cls):
        with open(cls.FILE_PATH, "w") as f:
            f.write(
                "This file is used to timeout the constructor of FunctionWithTimingOutConstructor."
            )


@cls()
class FunctionWithRetryCountingConstructor:
    COUNTER_FILE_PATH = "/tmp/FunctionWithRetryCountingConstructor_counter"
    MAX_RETRIES = 3

    def __init__(self):
        # Read current constructor run count, increment it, and write back
        constructor_run_count = self.get_constructor_run_count()
        constructor_run_count += 1

        with open(self.COUNTER_FILE_PATH, "w") as f:
            f.write(str(constructor_run_count))

        # Fail until we've retried the specified number of times
        if constructor_run_count <= self.MAX_RETRIES:
            raise Exception(f"Constructor failed on attempt {constructor_run_count}")

    @application()
    @function(retries=Retries(max_retries=MAX_RETRIES))
    def run(self, x: int) -> str:
        return "success after retries"

    @classmethod
    def reset_counter(cls):
        if os.path.exists(cls.COUNTER_FILE_PATH):
            os.remove(cls.COUNTER_FILE_PATH)

    @classmethod
    def get_constructor_run_count(cls):
        try:
            with open(cls.COUNTER_FILE_PATH, "r") as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return 0


@cls(init_timeout=1)
class FunctionWithRetryCountingTimeoutConstructor:
    COUNTER_FILE_PATH = "/tmp/FunctionWithRetryCountingTimeoutConstructor_counter"
    MAX_RETRIES = 3

    def __init__(self):
        super().__init__()
        # Read current constructor run count, increment it, and write back
        constructor_run_count = self.get_constructor_run_count()
        constructor_run_count += 1

        with open(self.COUNTER_FILE_PATH, "w") as f:
            f.write(str(constructor_run_count))

        # Timeout until we've retried the specified number of times
        if constructor_run_count <= self.MAX_RETRIES:
            time.sleep(1000)  # This will cause a timeout

    @application()
    @function(retries=Retries(max_retries=MAX_RETRIES))
    def run(self, x: int) -> str:
        return "success after timeout retries"

    @classmethod
    def reset_counter(cls):
        if os.path.exists(cls.COUNTER_FILE_PATH):
            os.remove(cls.COUNTER_FILE_PATH)

    @classmethod
    def get_constructor_run_count(cls):
        try:
            with open(cls.COUNTER_FILE_PATH, "r") as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return 0


@unittest.skip("Function Executor startup retries with delay is not implemented")
class TestFunctionConstructorRetries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_function_constructor_succeeds_after_failing_for_5_secs(self):
        def unfail_constructor_with_delay():
            time.sleep(5)
            FunctionWithFailingConstructor.unfail_constructor()

        FunctionWithFailingConstructor.fail_constructor()
        threading.Thread(target=unfail_constructor_with_delay).start()
        request: Request = run_remote_application(FunctionWithFailingConstructor.run, 1)
        self.assertEqual(request.output(), "success")

    def test_function_constructor_succeeds_after_timing_out_for_5_secs(self):
        def untimeout_constructor_with_delay():
            time.sleep(5)
            FunctionWithTimingOutConstructor.untimeout_constructor()

        FunctionWithTimingOutConstructor.timeout_constructor()
        threading.Thread(target=untimeout_constructor_with_delay).start()
        request: Request = run_remote_application(
            FunctionWithTimingOutConstructor.run, 1
        )
        self.assertEqual(request.output(), "success")


class TestFunctionConstructorRetriesWithCounter(unittest.TestCase):
    def setUp(self):
        deploy_applications(__file__)

    def test_function_constructor_succeeds_after_specified_retries(self):
        # Reset counter before starting test
        FunctionWithRetryCountingConstructor.reset_counter()

        # Run the app - it should succeed after exactly MAX_RETRIES + 1 attempts
        request: Request = run_remote_application(
            FunctionWithRetryCountingConstructor.run, 1
        )
        self.assertEqual(request.output(), "success after retries")

        # Verify the retry count matches expected retries + initial attempt
        constructor_run_count = (
            FunctionWithRetryCountingConstructor.get_constructor_run_count()
        )
        expected_count = FunctionWithRetryCountingConstructor.MAX_RETRIES + 1
        self.assertEqual(
            constructor_run_count,
            expected_count,
            f"Expected {expected_count} attempts (1 initial + {FunctionWithRetryCountingConstructor.MAX_RETRIES} retries), got {constructor_run_count}",
        )

    def test_function_constructor_timeout_succeeds_after_specified_retries(self):
        # Reset counter before starting test
        FunctionWithRetryCountingTimeoutConstructor.reset_counter()

        # Run the graph - it should succeed after exactly MAX_RETRIES + 1 attempts
        request: Request = run_remote_application(
            FunctionWithRetryCountingTimeoutConstructor.run, 1
        )

        # Verify the output
        self.assertEqual(request.output(), "success after timeout retries")

        # Verify the retry count matches expected retries + initial attempt
        constructor_run_count: int = (
            FunctionWithRetryCountingTimeoutConstructor.get_constructor_run_count()
        )
        expected_count = FunctionWithRetryCountingTimeoutConstructor.MAX_RETRIES + 1
        self.assertEqual(
            constructor_run_count,
            expected_count,
            f"Expected {expected_count} attempts (1 initial + {FunctionWithRetryCountingTimeoutConstructor.MAX_RETRIES} retries), got {constructor_run_count}",
        )


if __name__ == "__main__":
    unittest.main()
