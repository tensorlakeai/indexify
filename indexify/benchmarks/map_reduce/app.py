import random
import time
from typing import List

from pydantic import BaseModel
from tensorlake.applications import application, function


class MappedItem(BaseModel):
    data: str
    delay: float
    completed_at: float


class ReducerAccumulator(BaseModel):
    num_maps: int
    results: List[MappedItem]


@application()
@function(description="Indexify map reduce benchmark API")
def indexify_map_reduce_benchmark_api(num_maps: int) -> ReducerAccumulator:
    print(f"{time.time()}: running benchmark with: {num_maps} map calls")

    # Use tail calls to get max cluster throughput.
    return reduce_function.awaitable.reduce(
        map_function.awaitable.map([f"map_item_{i}" for i in range(num_maps)]),
        ReducerAccumulator(num_maps=0, results=[]),
    )


@function()
def map_function(data: str) -> MappedItem:
    print(f"{time.time()}: map_function: {data}")
    # Random delay between 0.1 and 2.0 seconds
    delay = random.uniform(0.1, 2.0)
    time.sleep(delay)
    return MappedItem(data=data, delay=delay, completed_at=time.time())


@function()
def reduce_function(acc: ReducerAccumulator, result: MappedItem) -> ReducerAccumulator:
    print(f"{time.time()}: reduce_function: {acc}, {result}")
    acc.results.append(result)
    acc.num_maps += 1
    return acc
