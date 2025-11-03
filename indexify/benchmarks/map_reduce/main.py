import argparse
import sys
import time

from app import indexify_map_reduce_benchmark_api
from tensorlake.applications import Request, run_remote_application
from tensorlake.applications.remote.deploy import deploy_applications

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Map Reduce benchmark")
    parser.add_argument(
        "--maps-count",
        type=int,
        default=1000,
        help="Number of map calls to make in each request",
    )
    parser.add_argument(
        "--num-requests",
        type=int,
        default=1,
        help="Number of requests to run",
    )
    parser.add_argument(
        "--report-output-file",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="File to write the test report output",
    )
    parser.add_argument(
        "--failure-threshold-seconds",
        type=int,
        default=120,
        help="Failure threshold in seconds for benchmark completion",
    )
    args = parser.parse_args()

    print("Deploying benchmark application...")
    deploy_applications(__file__)

    print(
        f"Starting map reduce benchmark with maps_count: {args.maps_count}, num_requests: {args.num_requests}"
    )

    # One map call results in one reducer call.
    total_function_calls: int = args.maps_count * 2 * args.num_requests
    start_time = time.time()
    requests: list[Request] = []
    for _ in range(args.num_requests):
        request: Request = run_remote_application(
            indexify_map_reduce_benchmark_api, args.maps_count
        )
        requests.append(request)
        print(f"Started request {request.id} with {args.maps_count} functions")

    print(
        f"All {len(requests)} requests started in {time.time() - start_time:.2f} seconds"
    )
    print("Waiting for all requests to complete...")
    for request in requests:
        request.output()

    total_time = time.time() - start_time
    print(
        f"""
Completed {len(requests)} requests in {total_time:.2f}s

Test Configuration:
- Requests: {args.num_requests}
- Map calls per request: {args.maps_count}
- Total function calls: {total_function_calls}
""",
        file=args.report_output_file,
    )

    if total_time > args.failure_threshold_seconds:
        print(
            f"Benchmark failed: total time {total_time:.2f}s exceeded failure threshold of {args.failure_threshold_seconds}s",
            file=args.report_output_file,
        )
        sys.exit(1)
