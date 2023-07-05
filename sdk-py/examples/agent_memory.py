from langchain.agents import ZeroShotAgent, Tool, AgentExecutor
from langchain import OpenAI, LLMChain
from langchain.utilities import GoogleSearchAPIWrapper

from indexify import IndexifyMemory, DEFAULT_INDEXIFY_URL

'''
For this script to work we need to set the following environment settings.
export GOOGLE_API_KEY=""
export GOOGLE_CSE_ID=""
export OPENAI_API_KEY=""

You can get the Google's CSE and API key from following URL's:
https://programmablesearchengine.google.com/controlpanel/create
https://console.cloud.google.com/projectselector2/google/maps-apis/credentials

OpenAI API Key from:
https://platform.openai.com/account/api-keys

Note: This example is a modification from langchain documentation here:
https://python.langchain.com/docs/modules/memory/how_to/agent_with_memory
'''

# Single line Magic to add vectorized memory!
memory = IndexifyMemory(memory_key="chat_history", indexify_url=DEFAULT_INDEXIFY_URL)

# Initialize Google search for the langchain.
search = GoogleSearchAPIWrapper()
tools = [
    Tool(
        name="Search",
        func=search.run,
        description="useful for when you need to answer questions about current events",
    )
]

prefix = """Have a conversation with a human, answering the following questions as best you can. You have access to the following tools:"""
suffix = """Begin!"

{chat_history}
Question: {input}
{agent_scratchpad}"""

prompt = ZeroShotAgent.create_prompt(
    tools,
    prefix=prefix,
    suffix=suffix,
    input_variables=["input", "chat_history", "agent_scratchpad"],
)

llm_chain = LLMChain(llm=OpenAI(temperature=0), prompt=prompt)
agent = ZeroShotAgent(llm_chain=llm_chain, tools=tools, verbose=True)
agent_chain = AgentExecutor.from_agent_and_tools(
    agent=agent, tools=tools, verbose=True, memory=memory
)


def ask(question):
    print(f"User question: {question}")
    agent_chain.run(input=question)


# Ask your questions off. Helper function just prints the question before starting the chain!
ask("How many people live in USA?")
ask("what is their national anthem called?")
ask("How many per state?")
ask("How many lines is the song?")
