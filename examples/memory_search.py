"""Example of using langchain agents with indexify memory and search."""

from langchain.agents import ZeroShotAgent, Tool, AgentExecutor
from langchain.memory import ConversationEntityMemory
from langchain import OpenAI, LLMChain

from indexify.indexify import Indexify, CreateIndexArgs, Metric, TextSplitter, IndexifyEntityStore, IndexifySearchTool


# Create an index
args = CreateIndexArgs(
    name='my_index',
    indexify_url='http://localhost:8900',
    embedding_model="all-minilm-l12-v2",
    metric=Metric.COSINE,
    splitter=TextSplitter.NEWLINE,
    unique_labels=["doc_name", "page_num"]
)
index = Indexify.create_index(*args)

# Create a memory store
db_url = "sqlite://indexify.db"
memory = ConversationEntityMemory(memory_key="chat_history", entity_store=IndexifyEntityStore(db_url))

# Use tool to search memory
search_tool = IndexifySearchTool(args.name, args.indexify_url)
tools = [
    Tool(
        name = "indexify_memory_search",
        func=search_tool.run,
        description="Search memory embeddings for information."
    )
]

# Create prompt style
prefix = """Have a conversation with a human, answering the following questions as best you can. You have access to the following tools:"""
suffix = """Begin!"

{chat_history}
Question: {input}
{agent_scratchpad}"""

prompt = ZeroShotAgent.create_prompt(
    tools, 
    prefix=prefix, 
    suffix=suffix, 
    input_variables=["input", "chat_history", "agent_scratchpad"]
)

# Create chain and agent
llm_chain = LLMChain(llm=OpenAI(temperature=0), prompt=prompt, memory=memory)
agent = ZeroShotAgent(llm_chain=llm_chain, tools=tools, verbose=True)
agent_chain = AgentExecutor.from_agent_and_tools(agent=agent, tools=tools, verbose=True, memory=memory)

agent_chain.run(input="What is the capital of France?")
    