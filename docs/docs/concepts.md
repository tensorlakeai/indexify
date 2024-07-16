# Key Concepts 

## Overview

Indexify is a powerful and versatile data framework designed to revolutionize the way we handle unstructured data for AI applications. It offers a seamless solution for building ingestion and extraction pipelines that can process various types of unstructured data, including documents, videos, images, and audio files.  A typical workflow involves:

1. Uploading unstructured data (documents, videos, images, audio)
2. Applying extractors to process the content
3. Updating vector indexes and structured stores
4. Retrieving information via semantic search on vector indexes and SQL queries on structured data tables


![Block Diagram](https://github.com/user-attachments/assets/c7715cc0-875c-4799-bb79-79b2f1cecd27)

## Core Components

To fully grasp the power and flexibility of Indexify, it's essential to understand its core concepts:

1. **Extractors**: These are the workhorses of Indexify. Extractors are functions that take data from upstream sources and produce three types of output:
   - Transformed data: For example, converting a PDF to plain text.
   - Embeddings: Vector representations of the data, useful for semantic search.
   - Structured data: Extracted metadata or features in a structured format.

2. **Extraction Graphs**: These are multi-step workflows created by chaining multiple extractors together. They allow you to define complex data processing pipelines that can handle various transformations and extractions in a single, cohesive flow.

3. **Namespaces**: Indexify uses namespaces as logical abstractions for storing related content. This feature allows for effective data partitioning based on security requirements or organizational boundaries, making it easier to manage large-scale data operations.

4. **Content**: In Indexify, 'Content' represents raw unstructured data. This could be documents, videos, images, or any other form of unstructured data. Content objects contain the raw bytes of the data along with metadata like MIME types.

5. **Vector Indexes**: These are automatically created from extractors that return embeddings. Vector indexes enable powerful semantic search capabilities, allowing you to find similar content based on meaning rather than just keywords.

6. **Structured Data Tables**: Metadata extracted from content is exposed via SQL queries. This allows for easy querying and analysis of the structured information derived from your unstructured data.



### 1. Extractor

An Extractor is essentially a Python class that can:

a) Transform unstructured data into intermediate forms
b) Extract features like embeddings or metadata (JSON) for LLM applications

Extractors consume `Content` which contains raw bytes of unstructured data, and they produce a list of Content and features from them.

![Image 4: Extractor_working](https://github.com/user-attachments/assets/ac12fc76-3043-485f-9a8b-6bbffa7d878d)

#### Types of Extraction:

1. **Transformation**
   - Converts a source into a list of `Content`
   - Example: PDF to text, video to audio
   - `Extractor(Content) -> List[Content]`
   
   ![Content Transformation](https://github.com/user-attachments/assets/f75ffbe4-fb8e-421f-be6b-ddc8d0e9d977)

2. **Structured Data Extraction**
   - Enriches content with structured data
   - Example: Adding bounding boxes to detected objects in an image
   - `Extractor(Content) -> List[Feature(Type=Metadata)]`
   
   ![Feature Extraction](https://github.com/user-attachments/assets/d60e6fff-9913-4033-8209-7200289b6ac9)

3. **Embedding Extraction**
   - Generates embeddings for ingested content
   - Indexify automatically creates indexes from these embeddings
   - `Extractor(Content) -> List[Feature(Type=Embedding)]`
   
   ![Embedding Extraction](https://github.com/user-attachments/assets/3797236a-7361-403d-a5ea-86230d21be47)

4. **Combined Extraction**
   - Performs transformation, embedding, and metadata extraction simultaneously
   - `Extractor(Content) -> List[Feature... Content ...]`

   ![Combined Extraction](https://github.com/user-attachments/assets/c704d6e8-9dd6-45b8-b770-b0092373fa5a)

Indexify allows you to have the freedom to either build custom extractors yourself, or make use of a wide array of pre-existing extractors.

| Modality | Extractor Name | Use Case | Supported Input Types |
|----------|----------------|----------|------------------------|
| Text | OpenAI | General-purpose text processing | `text/plain`, `application/pdf` |
| Text | Chunking | Text splitting into smaller chunks | `text/plain` |
| Image | Gemini | General-purpose image processing | `image/jpeg`, `image/png` |
| Image | EasyOCR | Text extraction from images using OCR | `image/jpeg`, `image/png` |
| PDF | PDFExtractor | Text, image, and table extraction from PDFs | `application/pdf` |
| PDF | Marker | PDF to markdown conversion | `application/pdf` |
| Audio | Whisper | Audio transcription | `audio`, `audio/mpeg` |
| Audio | ASR Diarization | Speech recognition and speaker diarization | `audio`, `audio/mpeg` |
| Presentation | PPT | Information extraction from presentations | `application/vnd.ms-powerpoint`, `application/vnd.openxmlformats-officedocument.presentationml.presentation` |

For an exhaustive list of all extractors and a guide on building custom extractors visit the official extractor [docs](https://docs.getindexify.ai/apis/extractors/).



### 2. Namespaces

- Logical abstractions for storing related content
- Allow data partitioning based on security and organizational boundaries

### 3. Content

- Representation of unstructured data (documents, video, images)

### 4. Extraction Graphs

- Apply a sequence of extractors on ingested content in a streaming manner
- Individual steps in an Extraction Graph are known as Extractors
- Track lineage of transformed content and extracted features
- Enable deletion of all transformed content and features when sources are deleted

![Extraction Policy](https://github.com/user-attachments/assets/e7649bd7-bb26-4873-8372-fb6367d3e5d8)

### 5. Vector Index and Retrieval APIs

- Automatically created from extractors that return embeddings
- Support various vector databases (Qdrant, Elastic Search, Open Search, PostgreSQL, LanceDB)
- Enable semantic/KNN search

### 6. Structured Data Tables

- Expose metadata extracted from content using SQL Queries
- Each Extraction Graph has a virtual SQL table
- Allow querying of metadata added to content

Example Usage:
```sql
select * from object_detector where object_name='ball'
```
This query retrieves all images with a detected ball, assuming an `object_detector` policy using YOLO object detection.

## Next Steps

To continue your journey with Indexify, consider exploring the following topics in order:

| Topics | Subtopics |
|--------|-----------|
| [Getting Started - Basic](https://docs.getindexify.ai/getting_started/) | - Setting up the Indexify Server<br>- Creating a Virtual Environment<br>- Downloading and Setting Up Extractors<br>- Defining Data Pipeline with YAML<br>- Loading Wikipedia Data<br>- Querying Indexed Data<br>- Building a Simple RAG Application |
| [Intermediate Use Case: Unstructured Data Extraction from a Tax PDF](https://docs.getindexify.ai/getting_started_intermediate/) | - Understanding the challenge of tax document processing<br>- Setting up an Indexify pipeline for PDF extraction<br>- Implementing extractors for key tax information<br>- Querying and retrieving processed tax data |
| [Key Concepts of Indexify](https://docs.getindexify.ai/concepts/) | - Extractors<br>  • Transformation<br>  • Structured Data Extraction<br>  • Embedding Extraction<br>  • Combined Transformation, Embedding, and Metadata Extraction<br>- Namespaces<br>- Content<br>- Extraction Graphs<br>- Vector Index and Retrieval APIs<br>- Structured Data Tables |
| [Architecture of Indexify](https://docs.getindexify.ai/architecture/) | - Indexify Server<br>  • Coordinator<br>  • Ingestion Server<br>- Extractors<br>- Deployment Layouts<br>  • Local Mode<br>  • Production Mode |
| [Building a Custom Extractor for Your Use Case](https://docs.getindexify.ai/apis/develop_extractors/) | - Understanding the Extractor SDK<br>- Designing your extractor's functionality<br>- Implementing the extractor class<br>- Testing and debugging your custom extractor<br>- Integrating the custom extractor into your Indexify pipeline |
| [Examples and Use Cases](https://docs.getindexify.ai/examples_index/) | - Document processing and analysis<br>- Image and video content extraction<br>- Audio transcription and analysis<br>- Multi-modal data processing<br>- Large-scale data ingestion and retrieval systems |

Each section builds upon the previous ones, providing a logical progression from practical application to deeper technical understanding and finally to customization and real-world examples.

For more information on how to use Indexify, refer to the [official documentation](https://docs.getindexify.ai/).

Happy coding!
