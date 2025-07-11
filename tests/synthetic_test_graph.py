#!/usr/bin/env python

import time
import random
import uuid
from tensorlake import tensorlake_function, Graph
from typing import List
from pydantic import BaseModel


class TestResult(BaseModel):
    results: List[dict] = []
    total_count: int = 0


# Module-scoped compute functions
@tensorlake_function()
def generate_work_items(num_functions: int) -> List[dict]:
    """Generate a list of work items - this will automatically fan out"""
    print(f"generate_work_items{num_functions}")
    return [{"id": i, "data": f"work_item_{i}"} for i in range(num_functions)]


@tensorlake_function()
def process_work_item(id: int, data: str) -> dict:
    """Process each work item (this runs in parallel for each item in the list)"""
    print(f"process_work_items")
    # Random delay between 0.1 and 2.0 seconds
    delay = random.uniform(0.1, 2.0)
    time.sleep(delay)
    return {"id": id, "data": data, "delay": delay, "completed_at": time.time()}


@tensorlake_function(accumulate=TestResult)
def collect_results(
    accumulator: TestResult, id: int, data: str, delay: float, completed_at: float
) -> TestResult:
    """Accumulate all results"""
    print(f"collect_results")
    result = {"id": id, "data": data, "delay": delay, "completed_at": completed_at}
    accumulator.results.append(result)
    accumulator.total_count += 1
    return accumulator


def create_synthetic_test_graph(num_functions: int = 3, namespace: str = None):
    """Create a synthetic test graph with fan-out behavior for testing"""

    # Build the graph
    version = str(uuid.uuid4())
    graph_name = f"synthetic_test_graph_{num_functions}_{version}"
    graph = Graph(
        start_node=generate_work_items,
        name=graph_name,
        description=f"Synthetic test graph with {num_functions} functions",
    )

    # Set namespace if provided
    if namespace:
        graph.namespace = namespace

    # Add edges
    graph.add_edge(generate_work_items, process_work_item)
    graph.add_edge(process_work_item, collect_results)

    return graph
