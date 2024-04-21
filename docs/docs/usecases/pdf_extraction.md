# PDF Extraction

PDF is a complex document type, and they can contain text, images and tabular data. Depending on the document type the strategy to extract information from PDF could vary. 

We have developed a PDF extractor which can extract text, images and tables from PDF documents. Additionaly, you can use many other PDF extraction libraries which we have 
packaged as an extractor. You can try out all the various extractors and see which one works best for your use-case.

Below is an overview of what you can achieve by combining the PDF Extractors, with Embedding, Chunking and other structured data extractors. We have some examples as well below.
![PDF Extraction High Level](../images/PDF_Usecase.png)

## Extractors
* [**tensorlake/pdf-extractor**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/pdf-extractor) - A combined PDF extractor which can extract text, image and tables
* [**tensorlake/ocrmypdf**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/ocrmypdf) - Uses the ocrmypdf library which uses tessarect under the hood to extract text from PDFs.
* [**tensorlake/easyocr**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/ocrpdf-gpu) - Uses EasyOCR to extract text from PDFs.

## Output Data Model
### Text
We extract text from PDFs as `Content` with text in the `data` attribute and the mime type is set to `text/plain`. 

### Image 
We extract images from PDFs as `Content` with bytes in the `data` attribute and the mime type is set to `image/png`. 

### Tables
Tables are extracted as JSON 

### Metadata
Every `Content` will have `page_number` as a metadata. 


## How to Test PDF Extraction

Download and Start the Indexify Server 
```bash
curl https://tensorlake.ai | sh
./indexify server -d
```

Run a PDF Extractor 
```bash
indexify-extractor download hub://pdf/pdf-extractor
indexify-extractor join-server pdf-extractor.pdf_extractor:PDFExtractor
```

Upload a PDF 

```python
client = IndexifyClient()
content_id = client.upload_file("foo.pdf")
client.create_extraction_policy(extractor="tensorlake/pdf-extractor", name="my-pdf-extractor")
```

Inspect the extracted content
```python
extracted_content = client.derived_content_of(content_id=content_id)
```



## Examples 

### Invoices
[Notebook for Invoices](../examples/Invoices.ipynb)

### Scientific Journals
[Notebook for Scientific Journals](../examples/Scientific_Journals.ipynb)

### SEC 10-K docs
[Notebook for SEC 10-K docs](../examples/SEC_10_K_docs.ipynb)

### Terms and Condition Documents of Car Rental
[Notebook for Documents of Car Rental](../examples/Terms_and_Condition_Documents_of_Car_Rental.ipynb)

### Terms and Conditions Documents of Health Care Benefits
[Notebook for Documents of Health Care](../examples/Terms_and_Conditions_Documents_of_Health_Care_Benefits.ipynb)
