import os
import requests
from indexify import IndexifyClient

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")

def retreive_chunks(pdf_path):
    client = IndexifyClient()
    
    # Upload the PDF file
    content_id = client.upload_file("pdf_chunker", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the chunked content
    chunks = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="pdf_chunker",
        policy_name="text_to_chunks"
    )
    
    return [chunk['content'].decode('utf-8') for chunk in chunks]

# Example usage
if __name__ == "__main__":
    pdf_url = "https://arxiv.org/pdf/2310.06825.pdf"
    pdf_path = "reference_document.pdf"
    
    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    
    # Chunk the PDF
    chunks = retreive_chunks(pdf_path)
    print(f"Number of chunks generated: {len(chunks)}")
    print("\nLast chunk:")
    print(chunks[0][:500] + "...")  # Print first 500 characters of the first chunk