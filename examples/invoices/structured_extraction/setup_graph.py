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

schema = Invoice.model_json_schema()

client = IndexifyClient()

extraction_graph_spec = f"""
name: 'pdf_schema_extractor'
extraction_policies:
  - extractor: 'tensorlake/marker'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/schema'
    name: 'text_to_schema'
    input_params:
      service: 'openai'
      model_name: 'gpt-4o-mini'
      key: 'YOUR_OPENAI_API_KEY'
      schema_config: {schema}
      additional_messages: 'Extract information in JSON according to this schema and return only the output. Do not include any explanations, only provide a  RFC8259 compliant JSON response.'
    content_source: 'pdf_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
