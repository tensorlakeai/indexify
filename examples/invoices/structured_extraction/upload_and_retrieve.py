import os
import requests
from indexify import IndexifyClient
import json

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")

def extract_schema_from_pdf(pdf_path):
    client = IndexifyClient()
    
    # Upload the PDF file
    content_id = client.upload_file("pdf_schema_extractor", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the extracted content
    extracted_data = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="pdf_schema_extractor",
        policy_name="text_to_schema"
    )
    
    return json.loads(extracted_data[0]['content'].decode('utf-8'))

# Example usage
if __name__ == "__main__":
    pdf_url = "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/Statement_HOA.pdf"
    pdf_path = "sample_invoice.pdf"
    
    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    
    # Extract schema-based information from the PDF
    extracted_info = extract_schema_from_pdf(pdf_path)
    print("Extracted information from the PDF:")
    print(json.dumps(extracted_info, indent=2))