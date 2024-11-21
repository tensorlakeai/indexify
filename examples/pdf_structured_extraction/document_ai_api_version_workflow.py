import io

from pydantic import BaseModel, Field
from decimal import Decimal
from datetime import date
from indexify.functions_sdk.data_objects import File
from indexify import indexify_function, Graph, RemoteGraph, Image
from typing import Optional, List, Any
import openai


image = (
    Image()
    .name("tensorlake/pdf-structured-extraction-inkwell-example")
    .base_image("python:3.11-slim")
    .run("apt update")
    .run("pip install openai")
    .run("pip install requests")
)


@indexify_function(image=image)
def tensorlake_document_ai_parse(file: File) -> str:
    """
    Call the inkwell parse API. TODO replace the token below for `Bearer`.
    :param file: The file data we want to parse.
    :return: The parsed data as a String.
    """
    import requests as rs

    # NOTE contact Tensorlake to get a key!
    PLATFORM_TOKEN = "EDIT-THIS"
    if PLATFORM_TOKEN == "EDIT-THIS":
        raise ValueError("Please fix the PLATFORM_TOKEN")

    url = "https://api.tensorlake.ai/documents/v1/upload"
    headers = {
        "Authorization": f"Bearer {PLATFORM_TOKEN}",
        "accept": "application/json"
    }

    files = {
        "file": ("document.pdf", io.BytesIO(file.data), 'application/pdf')
    }

    try:
        resp = rs.post(url, headers=headers, files=files).json()
        upload_url = resp['filename']
    except Exception as e:
        raise ValueError("Unable to upload file to Tensorlake DocumentAI.")

    parse_url = "https://api.tensorlake.ai/documents/v1/parse"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {PLATFORM_TOKEN}"
    }

    payload = {
        "file_path": upload_url,
        "output_mode": "markdown",
        "parse_mode": "large",
    }

    response = rs.post(parse_url, headers=headers, json=payload)

    if response.status_code != 200:
        print(f"Call to Parse API failed, received: {response.content}")
        return None

    resp_json = response.json()
    if resp_json["status"] == "successful":
        # Only return data if the job is successful.
        return resp_json["result"]

    return None


class BillSchema(BaseModel):
    account_no: str = Field(..., description="Account number of the customer")
    address: str = Field(..., description="Address of the customer")
    statement_date: Optional[date] = Field(..., description="Date when the statement was issued")
    due_date: str = Field(..., description="Date by which the payment is due")
    total_amount_due: float = Field(..., description="Total amount due on the bill", ge=0)
    payment_received_since_last_statement: float = Field(..., description="Payment received since the last statement", ge=0)
    current_delivery_charges: float = Field(..., description="Current delivery charges", ge=0)

    class Config:
        populate_by_name = True
        json_encoders = {
            date: lambda v: v.strftime("%m-%d-%Y"),
            Decimal: lambda v: str(v)
        }


def _create_message(system_prompt: str, user_prompt: str, markdown: str) -> List[Any]:
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                { "type": "text", "text": user_prompt},
                { "type": "text", "text": markdown},
            ],
        },
    ]


def _call_oai_client(system_prompt: str, user_prompt: str, markdown: str) -> str:
    OPENAI_API_KEY = "EDIT_ADD_KEY"

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    messages = _create_message(system_prompt, user_prompt, markdown)
    response = client.chat.completions.create(
        model="gpt-4o-mini", messages=messages
    )

    return response.choices[0].message.content


@indexify_function(image=image)
def extract_with_oai(markdown: str) -> BillSchema:
    schema = BillSchema.model_json_schema()

    SYSTEM_PROMPT = """You are an expert in Extracting data from a PDF file. You are given markdown text 
    extracted from a bill. You need to extract the data from this markdown text input. If there is a specific
    requirement or customization in further instructions, please follow them. Do no make a mistake in following the 
    instruction. If you're not able to extract some fields simply return the Optional value for that type as per the 
    schema."""

    USER_PROMPT = f"""Here is the markdown parsed from a bill. Extract the data from the bill according to the schema provided.

    Return the data in JSON format, return just the JSON object, and nothing else.

    Here is the schema:

    {schema}
    """

    resp = _call_oai_client(SYSTEM_PROMPT, USER_PROMPT, markdown)

    result = resp.replace("```json", "").replace("```", "")
    print(result)

    return BillSchema.model_validate_json(result)


def create_graph() -> Graph:
    g = Graph(name="bill_workflow_document_ai_version", start_node=tensorlake_document_ai_parse)

    g.add_edge(tensorlake_document_ai_parse, extract_with_oai)
    return g


if __name__ == "__main__":
    graph = create_graph()

    import sys
    graph = RemoteGraph.deploy(graph, additional_modules=[sys.modules[__name__]], server_url="http://100.106.216.46:8900")

    import httpx
    response = httpx.get("https://pub-5dc4d0c0254749378ccbcfffa4bd2a1e.r2.dev/sample_bill.pdf")

    f = File(data=response.content)

    print("Calling Graph")
    invocation_id = graph.run(block_until_done=True,file=f)

    data = graph.output(invocation_id, "extract_with_oai")
    print(data)
