# Available Extractors

Indexify comes with some pre-built extractors that you can download and deploy with your installation. We keep these extractors always updated with new releases of models and our internal code.

## Video
#### [Frames](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/frames)
Dissect a video into its individual frames and generate JPEG image content for each extracted frame. Comes with frame frequency and scene detection configuration that can be changed with input parameters.

#### [Audio](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/audio-extractor)
Extract the audio from a video file.

#### [Face](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/face-extractor)
Extract unique faces from video. This extractor uses face_detection to locate and extract facial features and sklearn DBSCAN to cluster and find uniqueness.


## PDF
#### [OCRMyPdf](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/ocrmypdf)
Extract text content from image based pdf files using this ocrmypdf based extractor.

#### [UnstructuredIO](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/unstructuredio)
This extractor uses unstructured.io to extract pieces of pdf document into separate plain text content data.

#### [LayoutLM Document QA](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/layoutlm_document_qa)
This is a fine-tuned version of the multi-modal [LayoutLM](https://aka.ms/layoutlm) model for the task of question answering on documents. It has been fine-tuned using both the SQuAD2.0 and [DocVQA](https://www.docvqa.org/) datasets.


## Invoice
#### [Donut CORD](https://github.com/tensorlakeai/indexify-extractors/tree/main/invoice-extractor/donut_cord)
This extractor parses pdf or image form of invoice which is provided in JSON format. It uses the pre-trained [donut cord fine-tune model from huggingface](https://huggingface.co/naver-clova-ix/donut-base-finetuned-cord-v2).
This model is specially good at extracting list of product and its price from invoice.

#### [Donut Invoice](https://github.com/tensorlakeai/indexify-extractors/tree/main/invoice-extractor/donut_invoice)
This extractor parses some invoice-related data from a PDF.
It uses the pre-trained [donut model from huggingface](https://huggingface.co/docs/transformers/model_doc/donut).


## Web Extractors

#### [Wikipedia](https://github.com/tensorlakeai/indexify-extractors/tree/main/web-extractors/wikipedia)
Extract text content from wikipedia html pages.