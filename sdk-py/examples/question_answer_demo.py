from indexify import AIndex, ARepository, TextChunk, DEFAULT_INDEXIFY_URL, wait_until
from datasets import load_dataset


class DemoQA:

    def __init__(self):
        self.repository = ARepository(DEFAULT_INDEXIFY_URL, "default")
        self.idx = AIndex(DEFAULT_INDEXIFY_URL, "default/default")

    def execute(self):
        # Add All Wikipedia articles
        datasets = load_dataset('squad', split='train')
        q_a_all = []
        futures = []
        for i in range(0, 10):
            context: str = datasets[i]["context"]
            question = datasets[i]["question"]
            answers = datasets[i]["answers"]
            futures.append(self.repository.add(TextChunk(context)))
            q_a_all.append((question, answers))
        wait_until(futures)
        for q_a in q_a_all:
            question = q_a[0]
            values = wait_until(self.idx.search(question, 1))
            print(f"Question: {question}, \nContext / Answer can be found in: {values[0].text}")


if __name__ == '__main__':
    demo = DemoQA()
    demo.execute()
