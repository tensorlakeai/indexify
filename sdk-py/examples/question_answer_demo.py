from indexify import AIndex, ARepository, TextChunk, DEFAULT_INDEXIFY_URL, wait_until, to_document
from datasets import load_dataset

from langchain.agents import ZeroShotAgent, Tool, AgentExecutor
from langchain import OpenAI, LLMChain
from langchain.utilities import GoogleSearchAPIWrapper
from langchain.chains.question_answering import load_qa_chain


class DemoQA:

    def __init__(self):
        self.repository = ARepository(DEFAULT_INDEXIFY_URL, "default")
        self.idx = AIndex(DEFAULT_INDEXIFY_URL, "default/default")
        self.llm_chain = load_qa_chain(OpenAI(temperature=0), chain_type="stuff")

    def execute(self):
        # TODO update with the latest news which LLM might not know answer to.

        # Add All squad articles
        datasets = load_dataset('squad', split='train')
        q_a_all = []
        futures = []
        print("Running QA example...\n Adding all squad context to the index...")
        for i in range(0, 10):
            context: str = datasets[i]["context"]
            question = datasets[i]["question"]
            answers = datasets[i]["answers"]
            futures.append(self.repository.add(TextChunk(context)))
            q_a_all.append((question, answers))
        wait_until(futures)
        print("Running extractors now... workaround for concurrency issues")
        resp = wait_until(self.repository.run_extractors("default"))
        print(f"number of extracted entities: {resp}")
        print("Starting search now...")
        for q_a in q_a_all:
            question = q_a[0]
            values = wait_until(self.idx.search(question, 2))
            print(f"Question: {question}, \nContext is in: {values[0].text}")
            answer = self.llm_chain.run(input_documents=[to_document(value) for value in values], question=question)
            print(f"Answer by OpenAI: {answer}")


if __name__ == '__main__':
    demo = DemoQA()
    demo.execute()
