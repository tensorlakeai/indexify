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
    content_id = client.upload_file("pdf_text_extractor", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the extracted text content
    extracted_text = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="pdf_text_extractor",
        policy_name="pdf_to_text"
    )
    
    return extracted_text[0]['content'].decode('utf-8')

# Example usage
if __name__ == "__main__":
    pdf_url = "https://arxiv.org/pdf/2310.06825.pdf"
    pdf_path = "reference_document.pdf"
    
    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    
    # Extract text from the PDF
    extracted_text = extract_text(pdf_path)
    print(f"Extracted text (first 500 characters):")
    print(extracted_text[:500] + "...")