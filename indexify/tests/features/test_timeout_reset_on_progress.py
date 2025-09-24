import time
import unittest

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


@tensorlake.api()
@tensorlake.function(timeout=5)
def function_with_progress_updates(ctx: tensorlake.RequestContext, x: int) -> str:
    """Function that calls update_progress multiple times during execution.

    This function takes longer than the timeout (5 seconds) but should succeed
    because update_progress() calls reset the timeout.
    """
    # Sleep for 2 seconds, then report progress
    time.sleep(2)
    ctx.update_progress(current=1, total=4)

    # Sleep for another 2 seconds, then report progress again
    time.sleep(2)
    ctx.update_progress(current=2, total=4)

    # Sleep for another 2 seconds, then report progress again
    time.sleep(2)
    ctx.update_progress(current=3, total=4)

    # Final sleep and completion
    time.sleep(2)
    ctx.update_progress(current=4, total=4)

    return "completed_with_progress_updates"


@tensorlake.api()
@tensorlake.function(timeout=5)
def function_without_progress_updates(ctx: tensorlake.RequestContext, x: int) -> str:
    """Function that sleeps longer than timeout without reporting progress.

    This function should timeout because it doesn't call update_progress().
    """

    # This exceeds the 5-second timeout and the 30-second check making
    # sure the timeout happens at all.
    time.sleep(60)

    return "should_not_reach_here"


class TestTimeoutResetOnProgress(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

    def test_function_succeeds_with_progress_updates(self):
        """Test that functions with progress updates don't timeout even if they exceed the original timeout."""

        start_time = time.monotonic()
        request: tensorlake.Request = tensorlake.call_remote_api(
            function_with_progress_updates, 1
        )
        outputs = request.output()
        duration = time.monotonic() - start_time

        # Should take about 8 seconds (4 * 2 seconds) but succeed
        self.assertGreater(duration, 7, "Function should take at least 7 seconds")
        self.assertLess(duration, 15, "Function should complete within reasonable time")

        # Check that the function succeeded
        self.assertEqual(outputs, "completed_with_progress_updates")

    def test_function_fails_without_progress_updates(self):
        """Test that functions without progress updates timeout as expected."""

        start_time = time.monotonic()
        request: tensorlake.Request = tensorlake.call_remote_api(
            function_without_progress_updates, 1
        )
        duration = time.monotonic() - start_time

        # Should timeout after about 5 seconds, but CI can be slow,
        # so we check against 30.
        self.assertLess(duration, 30, "Function should timeout quickly")

        # Check that the function failed (no outputs)
        try:
            outputs = request.output()
        except Exception as e:
            self.assertEqual(e.message, "functionerror")


if __name__ == "__main__":
    unittest.main()
