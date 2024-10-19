from pydantic import BaseModel, Field
from decimal import Decimal
from datetime import date
import tempfile
from indexify.functions_sdk.data_objects import File
from indexify import indexify_function, Graph, RemoteGraph, Image
from typing import Optional

image = (
    Image()
    .name("tensorlake/blueprint-pdf-structured-extraction")
    .base_image("pytorch/pytorch:2.4.1-cuda11.8-cudnn9-runtime")
    .run("apt update")
    .run("apt install -y libgl1-mesa-glx git g++")
    .run("pip install openai")
    .run("pip install psycopg2-binary")
    .run("pip install sqlmodel")
    .run("pip install langchain")
    .run("pip install git+https://github.com/facebookresearch/detectron2.git@v0.6")
    .run("apt install -y tesseract-ocr")
    .run("apt install -y libtesseract-dev")
    .run('pip install "py-inkwell[inference]"')
)
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

@indexify_function(image=image)
def parse_pdf(file: File) -> BillSchema:
    from inkwell.ocr import OCRType, OCRFactory
    from inkwell.io import read_pdf_as_images
    schema = BillSchema.model_json_schema()

    USER_PROMPT = f"""Here is the image of a bill. Extract the data from the bill according to the schema provided.

    Return the data in JSON format, return just the JSON object, and nothing else.

    Here is the schema:

    {schema}
    """
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".pdf") as f:
        f.write(file.data)
        ocr = OCRFactory.get_ocr(
            OCRType.OPENAI_GPT4O_MINI,
        )
        images = read_pdf_as_images(f.name)
        result = ocr.process(images[0], user_prompt=USER_PROMPT).replace("```json", "").replace("```", "")
        print(result)
        return BillSchema.model_validate_json(result)

@indexify_function(image=image)
def write_to_db(bill: BillSchema) -> None:
    from sqlmodel import Field, SQLModel, Session, create_engine
    from typing import Optional
    from datetime import date
    engine = create_engine("postgresql://db_user:db_password@postgres:5432/indexify_demo")
    class Bill(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        account_no: str = Field(..., description="Account number of the customer")
        address: str = Field(..., description="Address of the customer")
        statement_date: Optional[date] = Field(..., description="Date when the statement was issued")
        due_date: date = Field(..., description="Date by which the payment is due")
        total_amount_due: float = Field(..., description="Total amount due on the bill", ge=0)
        payment_received_since_last_statement: float = Field(..., description="Payment received since the last statement", ge=0)
        current_delivery_charges: float = Field(..., description="Current delivery charges", ge=0)


    SQLModel.metadata.create_all(engine)
    bill = Bill(**bill.dict())
    with Session(engine) as session:
        session.add(bill)
        session.commit()

def create_graph() -> Graph:
    g = Graph(name="bill_workflow", start_node=parse_pdf)
    g.add_edge(parse_pdf, write_to_db)
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
    structured_data = graph.output(invocation_id, "parse_pdf")
    print(structured_data)
