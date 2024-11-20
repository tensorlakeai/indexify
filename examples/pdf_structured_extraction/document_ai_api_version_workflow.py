from pydantic import BaseModel, Field
from decimal import Decimal
from datetime import date
import tempfile
from indexify.functions_sdk.data_objects import File
from indexify import indexify_function, Graph, RemoteGraph, Image
from typing import Optional


requests_image = (
    Image()
    .name("tensorlake/requests-image")
    .run("pip install requests")
)

@indexify_function(image=requests_image)
def tensorlake_document_ai_parse(file: File) -> str:
    """
    Call the inkwell parse API. TODO replace the token below for `Bearer`.
    :param file: The file data we want to parse.
    :return: The parsed data as a String.
    """
    import requests as rs

    url = "https://api.tensorlake.ai/documents/v1/upload"
    headers = {
        "Authorization": "Bearer <EDIT THIS>",
        "accept": "application/json"
    }

    with tempfile.NamedTemporaryFile(mode="wb", suffix=".pdf") as f:
        f.write(file.data)

        files = {
            "file": (f.name, f)
        }

        try:
            upload_url = rs.post(url, headers=headers, files=files).json()['filename']
        except Exception as e:
            raise ValueError("Unable to upload file to Tensorlake DocumentAI.")


    url = "https://api.tensorlake.ai/documents/v1/parse"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer <EDIT THIS>"
    }

    payload = {
        "file_path": upload_url,
        "output_mode": "markdown",
        "parse_mode": "large",
    }

    response = rs.post(url, headers=headers, json=payload)

    resp_json = response.json()
    if resp_json["status"] == "successful":
        # Only return data if the job is successful.
        return resp_json["result"]

    return None


class BillSchema(BaseModel):
    account_no: str = Field(..., description="Account number of the customer")
    address: str = Field(..., description="Address of the customer")
    statement_date: Optional[date] = Field(..., description="Date when the statement was issued")
    due_date: date = Field(..., description="Date by which the payment is due")
    total_amount_due: float = Field(..., description="Total amount due on the bill", ge=0)
    payment_received_since_last_statement: float = Field(..., description="Payment received since the last statement", ge=0)
    current_delivery_charges: float = Field(..., description="Current delivery charges", ge=0)

    class Config:
        populate_by_name = True
        json_encoders = {
            date: lambda v: v.strftime("%m-%d-%Y"),
            Decimal: lambda v: str(v)
        }

def create_graph() -> Graph:
    g = Graph(name="bill_workflow_document_ai_version", start_node=tensorlake_document_ai_parse)

    # USER TODO - Call the structured parsing API as needed in the follow commented out line.
    # g.add_edge(tensorlake_document_ai_parse, write_to_db)
    return g


if __name__ == "__main__":
    from pathlib import Path
    g = create_graph()
    import sys
    graph = RemoteGraph.deploy(g, additional_modules=[sys.modules[__name__]], server_url="http://100.106.216.46:8900")
    import httpx
    response = httpx.get("https://pub-5dc4d0c0254749378ccbcfffa4bd2a1e.r2.dev/sample_bill.pdf")
    f = File(data=response.content)
    invocation_id = graph.run(block_until_done=True,file=f)
    data = graph.output(invocation_id, "tensorlake_document_ai_parse")
    print(data)
