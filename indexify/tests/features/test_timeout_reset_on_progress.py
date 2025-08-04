import time
import unittest

from tensorlake import (
    Graph,
    GraphRequestContext,
    tensorlake_function,
)
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from testing import remote_or_local_graph, test_graph_name


@tensorlake_function(inject_ctx=True, timeout=5)
def function_with_progress_updates(ctx: GraphRequestContext, x: int) -> str:
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


@tensorlake_function(inject_ctx=True, timeout=5)
def function_without_progress_updates(ctx: GraphRequestContext, x: int) -> str:
    """Function that sleeps longer than timeout without reporting progress.

    This function should timeout because it doesn't call update_progress().
    """

    # This exceeds the 5-second timeout and the 30-second check making
    # sure the timeout happens at all.
    time.sleep(60)

    return "should_not_reach_here"


class TestTimeoutResetOnProgress(unittest.TestCase):
    def test_function_succeeds_with_progress_updates(self):
        """Test that functions with progress updates don't timeout even if they exceed the original timeout."""
        graph = Graph(
            name=test_graph_name(self),
            description="test timeout reset on progress",
            start_node=function_with_progress_updates,
        )
        graph = remote_or_local_graph(graph, remote=True)

        start_time = time.monotonic()
        invocation_id = graph.run(block_until_done=True, x=1)
        duration = time.monotonic() - start_time

        # Should take about 8 seconds (4 * 2 seconds) but succeed
        self.assertGreater(duration, 7, "Function should take at least 7 seconds")
        self.assertLess(duration, 15, "Function should complete within reasonable time")

        # Check that the function succeeded
        outputs = graph.output(invocation_id, function_with_progress_updates.name)
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0], "completed_with_progress_updates")

    def test_function_fails_without_progress_updates(self):
        """Test that functions without progress updates timeout as expected."""
        graph = Graph(
            name=test_graph_name(self),
            description="test timeout without progress",
            start_node=function_without_progress_updates,
        )
        graph = remote_or_local_graph(graph, remote=True)

        start_time = time.monotonic()
        invocation_id = graph.run(block_until_done=True, x=1)
        duration = time.monotonic() - start_time

        # Should timeout after about 5 seconds, but CI can be slow,
        # so we check against 30.
        self.assertLess(duration, 30, "Function should timeout quickly")

        # Check that the function failed (no outputs)
        outputs = graph.output(invocation_id, function_without_progress_updates.name)
        self.assertEqual(len(outputs), 0, "Function should have failed due to timeout")


if __name__ == "__main__":
    unittest.main()
