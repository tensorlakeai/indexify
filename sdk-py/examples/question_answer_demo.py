from indexify import Index, TextChunk
from datasets import load_dataset


class DemoQA:
    def __init__(self):
        self.idx = Index(Index.DEFAULT_INDEXIFY_URL, "default/default")

    def execute(self):
        # Add All Wikipedia articles
        datasets = load_dataset('squad', split='train')
        q_a_all = []
        for i in range(0, 10):
            context: str = datasets[i]["context"]
            question = datasets[i]["question"]
            answers = datasets[i]["answers"]
            self.idx.add(TextChunk(context))
            q_a_all.append((question, answers))
        for q_a in q_a_all:
            question = q_a[0]
            values = self.idx.search(question, 1)
            print(f"Question: {question}, \nContext / Answer can be found in: {values[0].text}")


if __name__ == '__main__':
    demo = DemoQA()
    demo.execute()
