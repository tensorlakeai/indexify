from openai import OpenAI
from indexify import IndexifyClient

client = IndexifyClient()
client_openai = OpenAI()

def get_context(question: str, index: str, top_k=3):
    results = client.search_index(name=index, query=question, top_k=3)
    context = ""
    for result in results:
        context = context + f"content id: {result['content_id']} \n\n passage: {result['text']}\n"
    return context

def create_prompt(question, context):
    return f"Answer the question, based on the context.\n question: {question} \n context: {context}"

question = "What are the tax brackets in California and how much would I owe on an income of $24,000?"
context = get_context(question, "pdfqa.pdfembedding.embedding")

prompt = create_prompt(question, context)

chat_completion = client_openai.chat.completions.create(
    messages=[
        {
            "role": "user",
            "content": prompt,
        }
    ],
    model="gpt-3.5-turbo",
)

print(chat_completion.choices[0].message.content)