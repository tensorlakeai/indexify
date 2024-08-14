import requests

from indexify import IndexifyClient
import os

client = IndexifyClient()

def upload(file_url):
    print(f"downloading file {file_url}", flush=True)
    response = requests.get(file_url)
    response.raise_for_status()
    with open("presentation.pptx", "wb") as f:
        f.write(response.content)
    content_id = client.upload_file("presentation_extraction", "presentation.pptx")
    return content_id

def retrieve(content_id):
    print(f"waiting for extraction of {content_id}", flush=True)
    client.wait_for_extraction(content_id)
    ppt_content = client.get_extracted_content(content_id, "presentation_extraction", "ppt_extraction")
    print(ppt_content)


if __name__ == "__main__":
    content_id = upload("")
    retrieve(content_id)
    os.remove("presentation.pptx")
