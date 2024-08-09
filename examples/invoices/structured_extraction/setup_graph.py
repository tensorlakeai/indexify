from indexify import IndexifyClient, ExtractionGraph
from pydantic import BaseModel

class Invoice(BaseModel):
    invoice_number: str
    date: str
    account_number: str
    owner: str
    address: str
    last_month_balance: str
    current_amount_due: str
    registration_key: str
    due_date: str

schema = Invoice.schema()
schema["additionalProperties"] = False

client = IndexifyClient()

extraction_graph_spec = f"""
name: 'pdf_schema_extractor'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_text'
    input_params:
      output_format: 'markdown'
  - extractor: 'tensorlake/schema'
    name: 'text_to_schema'
    input_params:
      model: 'gpt-4o-2024-08-06'
      response_format: {schema}
    content_source: 'pdf_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
