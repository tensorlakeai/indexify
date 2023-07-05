import argparse
import time

from indexify import AIndex, ARepository, AMemory, TextChunk, DEFAULT_INDEXIFY_URL, wait_until, Message
from datasets import load_dataset


class BulkUploadRepository:

    def __init__(self, url):
        self.url = url
        self.repository = ARepository(url)
        self.idx = AIndex(url)

    def benchmark_repository(self, concurrency: int, loop: int):
        datasets = load_dataset('squad', split='train')
        for j in range(0, loop):
            start_time = time.time()
            futures = []
            for i in range(0, concurrency):
                context: str = datasets[i]["context"]
                futures.append(self.repository.add(TextChunk(context)))
            wait_until(futures)
            print(f"repository.add seconds: {(time.time() - start_time)}")

        print("Running extractors now... workaround for concurrency issues")
        resp = wait_until(self.repository.run_extractors("default"))
        print(f"number of extracted entities: {resp}")

        for j in range(0, loop):
            start_time = time.time()
            futures = []
            for i in range(0, concurrency):
                question = datasets[i]["question"]
                futures.append(self.idx.search(question, 1))
            wait_until(futures)
            print(f"repository.search seconds: {(time.time() - start_time)}")

    def benchmark_memory(self, concurrency: int, loop: int):
        for j in range(0, loop):
            start_time = time.time()
            memory_list = []
            for i in range(0, concurrency):
                memory_list.append(AMemory(self.url))
            futures_create = []
            for i in range(0, concurrency):
                futures_create.append(memory_list[i].create())
            wait_until(futures_create)
            print(f"memory.create seconds: {(time.time() - start_time)}")
            futures_add = []
            for i in range(0, concurrency):
                futures_add.append(memory_list[i].add(Message("human", "Indexify is amazing!"),
                                                      Message("assistant", "How are you planning on using Indexify?!")))
            wait_until(futures_add)
            print(f"memory.add seconds: {(time.time() - start_time)}")
            futures_all = []
            for i in range(0, concurrency):
                futures_all.append(self._memory.all())
            wait_until(futures_all)
            print(f"memory.all seconds: {(time.time() - start_time)}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", default=DEFAULT_INDEXIFY_URL, help="indexify url to connect to")
    parser.add_argument("-c", "--concurrency", default=10,
                        help="We deal with Async functions its how often we wait")
    parser.add_argument("-l", "--loop", default=100,
                        help="Number of loops with concurrency set as per -c")
    args = parser.parse_args()
    print(f"looping for {args.loop}, with concurrency is set to be: {args.concurrency}, "
          f"will run {args.loop * args.concurrency} requests")
    demo = BulkUploadRepository(args.url)
    demo.benchmark_repository(args.concurrency, args.loop)
    demo.benchmark_memory(args.concurrency, args.loop)
