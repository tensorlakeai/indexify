#!/usr/bin/env python

import pytest
import tensorlake
import time
from tensorlake.http_client import InvocationFinishedEvent
import threading
from collections import defaultdict
from rich import print


@pytest.mark.integration
def test_synthetic_graph_workflow(
    test_graphs, function_counts, num_invocations, timeout, record_testsuite_property
):
    # Get the list of graphs to test
    graphs = test_graphs
    expected_total_invocations = len(graphs) * num_invocations

    # Start invocations using streaming API
    print(
        f"Starting {expected_total_invocations} parallel invocations ({num_invocations} per graph)..."
    )
    start_time = time.time()

    # Track invocations and their completion
    invocation_streams = []
    completed_invocations = set()
    results = {}
    invocation_times = {}  # Track start and end times for each invocation

    def stream_invocation(graph, num_functions, graph_index, invocation_index):
        """Stream a single invocation and collect its events"""
        try:
            stream = graph.stream(block_until_done=True, num_functions=num_functions)
            invocation_id = None

            for event in stream:
                if invocation_id is None:
                    invocation_id = event.payload.invocation_id
                    invocation_start_time = time.time()
                    invocation_times[invocation_id] = {"start": invocation_start_time}
                    print(
                        f"Started invocation {len(invocation_streams)+1}/{expected_total_invocations} on graph{graph_index+1} (num_functions={num_functions}): {invocation_id}"
                    )

                # Print the event for tracing
                print(f"[{invocation_id}] {event}")

                # Check if this is an invocation finished event
                if isinstance(event.payload, InvocationFinishedEvent):
                    invocation_end_time = time.time()
                    invocation_times[invocation_id]["end"] = invocation_end_time
                    invocation_duration = (
                        invocation_end_time - invocation_times[invocation_id]["start"]
                    )
                    print(
                        f"Invocation {invocation_id} completed after {time.time() - start_time:.2f} seconds (duration: {invocation_duration:.2f}s)"
                    )

                    # Get the final result (returns list with TestResult object)
                    result_list = graph.output(invocation_id, "collect_results")

                    # Validate result structure
                    assert result_list is not None
                    assert len(result_list) == 1
                    result = result_list[0]  # Extract the TestResult object
                    assert hasattr(result, "results")
                    assert hasattr(result, "total_count")

                    results[invocation_id] = result
                    completed_invocations.add(invocation_id)
                    break

        except Exception as e:
            print(f"Error in invocation stream: {e}")
            raise

    # Start all invocations in parallel using threads
    threads = []
    for i, graph in enumerate(graphs):
        num_functions = function_counts[i]
        for j in range(num_invocations):
            thread = threading.Thread(
                target=stream_invocation, args=(graph, num_functions, i, j)
            )
            thread.start()
            threads.append(thread)
            invocation_streams.append(thread)

    print(
        f"All {expected_total_invocations} invocations started in {time.time() - start_time:.2f} seconds"
    )

    # Wait for all threads to complete or timeout
    for thread in threads:
        remaining_time = timeout - (time.time() - start_time)
        if remaining_time > 0:
            thread.join(timeout=remaining_time)
        else:
            break

    total_time = time.time() - start_time
    completed = len(completed_invocations)
    print(f"All {completed} invocations completed in {total_time:.2f} seconds")

    # Calculate invocation timing statistics
    invocation_durations = []
    for inv_id, times in invocation_times.items():
        if "end" in times:
            duration = times["end"] - times["start"]
            invocation_durations.append(duration)

    if invocation_durations:
        min_time = min(invocation_durations)
        max_time = max(invocation_durations)
        avg_time = sum(invocation_durations) / len(invocation_durations)
        parallelization_efficiency = sum(invocation_durations) / total_time

        # Record properties for test suite reporting
        record_testsuite_property("total_test_time", f"{total_time:.2f}")
        record_testsuite_property("min_invocation_time", f"{min_time:.2f}")
        record_testsuite_property("max_invocation_time", f"{max_time:.2f}")
        record_testsuite_property("avg_invocation_time", f"{avg_time:.2f}")
        record_testsuite_property(
            "parallelization_efficiency", f"{parallelization_efficiency:.1f}"
        )
        record_testsuite_property("total_invocations", str(expected_total_invocations))
        record_testsuite_property(
            "function_counts", ",".join(map(str, function_counts))
        )
        record_testsuite_property("num_invocations_per_graph", str(num_invocations))

        print(f"\n📊 Invocation Statistics:")
        print(f"  Total test time: {total_time:.2f}s")
        print(f"  Individual invocation times:")
        print(f"    Min: {min_time:.2f}s")
        print(f"    Max: {max_time:.2f}s")
        print(f"    Avg: {avg_time:.2f}s")
        print(f"  Parallelization efficiency: {parallelization_efficiency:.1f}x")

    # Verify all completed
    assert (
        completed == expected_total_invocations
    ), f"Only {completed} out of {expected_total_invocations} invocations completed"

    # Verify results
    assert len(results) == expected_total_invocations

    # Dynamically check results for each function count
    for expected_count in function_counts:
        matching_results = [
            r for r in results.values() if r.total_count == expected_count
        ]
        assert (
            len(matching_results) == num_invocations
        ), f"Expected {num_invocations} results with {expected_count} functions, got {len(matching_results)}"

    print(
        f"\n✅ All tests passed! Successfully ran {expected_total_invocations} parallel invocations across {len(graphs)} graphs with function counts: {function_counts}."
    )
