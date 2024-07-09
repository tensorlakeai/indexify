# PDF Extractors

PDF is the most complex and multi-layered source of unstructured data extraction. Extraction is not always straightforward and easy. Indexify gives you the freedom to choose between different extractors based on your use case and source of data in the PDF. If you want to learn more about extractors, their design and usage, read the Indexify [documentation](https://docs.getindexify.ai/concepts/).

| Extractor Name | Use Case | Supported Input Types |
|----------------|----------|------------------------|
| EasyOCR | Text extraction from PDFs and images using OCR | `application/pdf`, `image/jpeg`, `image/png` |
| PDFExtractor | Text, image, and table extraction from PDFs | `application/pdf` |
| OCRMyPDF | Text extraction from image-based PDFs | `application/pdf` |
| UnstructuredIO | Structured extraction of PDF content | `application/pdf` |
| LayoutLM Document QA | Question answering on PDF documents | `application/pdf`, `image/jpeg`, `image/png` |
| Marker | PDF, EPUB, and MOBI to markdown conversion | `application/pdf` |
| PaddleOCR | Text extraction from PDFs using PaddleOCR | `application/pdf` |
| PPT | Information extraction from presentations | `application/vnd.ms-powerpoint`, `application/vnd.openxmlformats-officedocument.presentationml.presentation` |

## [EasyOCR](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/easyocrpdf)

### Description
This extractor uses EasyOCR to generate searchable PDF content from a regular PDF and then extract the text into plain text content.

### Input Parameters
- None specified

### Input Data Types
```
["application/pdf", "image/jpeg", "image/png"]
```

## [PDFExtractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/pdfextractor)

### Description
Extract text, images, and tables as strings, bytes, and JSON respectively using this extractor.

### Input Parameters
- None specified
- **output_types** (default: ["text"]): List of output types to extract (options: "text", "image", "table")

### Input Data Types
```
["application/pdf"]
```

## [OCRMyPDF](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/ocrmypdf)

### Description
Extract text content from image-based PDF files using this OCRMyPDF-based extractor.

### Input Parameters
- **deskew** (optional): Whether to deskew the PDF
- **language** (optional): List of languages for OCR
- **remove_background** (optional): Whether to remove the background
- **rotate_pages** (optional): Whether to rotate pages
- **skip_text** (optional, default: True): Whether to skip text extraction
- **output_types** (default: ["text"]): List of output types to extract (options: "text")

### Input Data Types
```
["application/pdf"]
```

## [UnstructuredIO](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/unstructuredio)

### Description
This extractor uses unstructured.io to extract pieces of PDF document into separate plain text content data.

### Input Parameters
- **strategy** (default: "auto"): Extraction strategy (options: "auto", "hi_res", "ocr_only", "fast")
- **hi_res_model_name** (default: "yolox"): High-resolution model name
- **infer_table_structure** (default: True): Whether to infer table structure
- **output_types** (default: ["text"]): List of output types to extract (options: "text", "table")

### Input Data Types
```
["application/pdf"]
```

## [LayoutLM Document QA](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/layoutlm_document_qa)

### Description
This is a fine-tuned version of the multi-modal [LayoutLM](https://aka.ms/layoutlm) model for the task of question answering on documents. It has been fine-tuned using both the SQuAD2.0 and [DocVQA](https://www.docvqa.org/) datasets.

### Input Parameters
- **query** (default: "What is the invoice total?"): The question to ask about the document
- **output_types** (default: ["text"]): List of output types to extract (options: "text")


### Input Data Types
```
["application/pdf", "image/jpeg", "image/png"]
```

## [Marker Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/marker)

### Description
Markdown extractor converts PDF, EPUB, and MOBI to markdown. It's 10x faster than nougat, more accurate on most documents, and has low hallucination risk.

### Input Parameters
- **max_pages** (optional): Maximum number of pages to process
- **start_page** (optional): Starting page for processing
- **langs** (optional): Languages to consider
- **batch_multiplier** (default: 2): Batch multiplier for processing
- **output_types** (default: ["text"]): List of output types to extract (options: "text", "image")

### Input Data Types
```
["application/pdf"]
```

## [PaddleOCR Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/paddleocr)

### Description
PDF Extractor for Texts using PaddleOCR.

### Input Parameters
- None specified
- **output_types** (default: ["text"]): List of output types to extract (options: "text")

### Input Data Types
```
["application/pdf"]
```

## [PPT Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/ppt)

### Description
An extractor that lets you extract information from presentations.

### Input Parameters
- None specified
- **output_types** (default: ["text", "table"]): List of output types to extract (options: "text", "table", "image")

### Input Data Types
```
["application/vnd.ms-powerpoint", "application/vnd.openxmlformats-officedocument.presentationml.presentation"]
```
