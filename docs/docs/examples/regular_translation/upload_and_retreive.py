import json
import os
import requests
from indexify import IndexifyClient

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")


def translate_pdf(pdf_path):
    client = IndexifyClient()
    
    # Upload the PDF file
    content_id = client.upload_file("pdf_translator_en_to_fr", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the translated content
    translated_content = client.get_extracted_content(
        content_id=content_id,
        graph_name="pdf_translator_en_to_fr",
        policy_name="text_to_french"
    )
    
    # Decode the translated content
    translated_text = translated_content[0]['content'].decode('utf-8')
    return translated_text

# Example usage
if __name__ == "__main__":
    pdf_url = "https://arxiv.org/pdf/2310.06825.pdf"
    pdf_path = "document_to_translate.pdf"

    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    translated_text = translate_pdf(pdf_path)
    
    print("Translated Text (first 500 characters):")
    print(translated_text[:500])

    # Optionally, save the translated text to a file
    with open("translated_document.txt", "w", encoding="utf-8") as f:
        f.write(translated_text)
    print("Full translation saved to 'translated_document.txt'")