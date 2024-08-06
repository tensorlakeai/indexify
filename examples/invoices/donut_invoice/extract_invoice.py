import requests
from indexify import IndexifyClient

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")

def extract_text(pdf_path):
    client = IndexifyClient()
    
    # Upload the PDF file
    content_id = client.upload_file("invoice_extractor", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the extracted text content
    extracted_text = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="invoice_extractor",
        policy_name="invoice_to_text"
    )
    extracted_text = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="invoice_extractor",
        policy_name="invoice_to_text"
    )
    
    return extracted_text

# Example usage
if __name__ == "__main__":
    pdf_url = "https://extractor-files.diptanu-6d5.workers.dev/invoice-example.pdf"
    pdf_path = "reference_document.pdf"
    
    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    
    # Extract text from the PDF
    extracted_text = extract_text(pdf_path)
    print(extracted_text)
