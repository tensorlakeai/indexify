import requests
from indexify import IndexifyClient

client = IndexifyClient()

response = requests.get("https://arev.assembly.ca.gov/sites/arev.assembly.ca.gov/files/publications/Chapter_2B.pdf")
with open("taxes.pdf", 'wb') as file:
    file.write(response.content)

client.upload_file("pdfqa", "taxes.pdf")