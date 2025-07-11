import pytest
import os
import uuid
import time
import random
import tensorlake
from tensorlake import RemoteGraph, Graph
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from typing import Union


def pytest_addoption(parser):
    parser.addoption(
        "--server-url",
        action="store",
        default="http://localhost:8900",
        help="Server URL for remote graphs",
    )
    parser.addoption(
        "--function-counts",
        action="store",
        default="3,5",
        help="Comma-separated list of function counts for test graphs (e.g., '3,5,10')",
    )
    parser.addoption(
        "--num-invocations",
        action="store",
        default="2",
        help="Number of invocations to run per graph",
    )
    parser.addoption(
        "--timeout",
        action="store",
        default="300",
        help="Timeout in seconds for test completion (default: 300 = 5 minutes)",
    )


@pytest.fixture
def server_url(request):
    return request.config.getoption("--server-url")


@pytest.fixture
def tensorlake_client(server_url):
    return tensorlake.TensorlakeClient(service_url=server_url)


@pytest.fixture
def function_counts(request):
    """Parse function counts from command line argument"""
    function_counts_str = request.config.getoption("--function-counts")
    return [int(count.strip()) for count in function_counts_str.split(",")]


@pytest.fixture
def num_invocations(request):
    """Get number of invocations from command line argument"""
    return int(request.config.getoption("--num-invocations"))


@pytest.fixture
def timeout(request):
    """Get timeout in seconds from command line argument"""
    return int(request.config.getoption("--timeout"))


@pytest.fixture
def test_graphs(tensorlake_client, function_counts):
    """Returns a list of graphs to run for testing"""
    created_graph_names = []

    def _create_graph(num_functions: int = 3, namespace: str = None):
        print(f"Deploying synthetic test graph with {num_functions} functions")

        # Generate UUID for version
        version = str(uuid.uuid4())

        tags = (
            dict(tag.split(":") for tag in os.environ.get("DEPLOYMENT_TAGS").split(","))
            if os.environ.get("DEPLOYMENT_TAGS")
            else {
                "hostname": os.uname().nodename,
            }
        )

        # Import and create graph from separate file
        from synthetic_test_graph import create_synthetic_test_graph

        graph = create_synthetic_test_graph(
            num_functions=num_functions, namespace=namespace
        )

        print(f"Deploying graph: {graph.name}")
        remote_graph = RemoteGraph.deploy(
            graph=graph,
            code_dir_path=graph_code_dir_path(__file__),
            client=tensorlake_client,
        )

        # Track created graph for cleanup
        created_graph_names.append(graph.name)

        return remote_graph

    # Create graphs based on command line function counts
    graphs = []
    for i, num_functions in enumerate(function_counts):
        namespace = f"ns{i+1}"
        graph = _create_graph(num_functions=num_functions, namespace=namespace)
        graphs.append(graph)

    yield graphs

    # Cleanup all created graphs
    for remote_graph_name in created_graph_names:
        try:
            print(f"Deleting graph: {remote_graph_name}")
            tensorlake_client.delete_compute_graph(remote_graph_name)
        except Exception as e:
            print(f"Warning: Failed to delete graph {remote_graph_name}: {e}")
