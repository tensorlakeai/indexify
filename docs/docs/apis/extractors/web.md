# Web Extractors

Web extractors are specialized tools designed to parse and extract structured information from web pages and HTML content. These extractors are crucial for tasks such as web scraping, content analysis, and data mining from online sources. They can process various elements of web pages, including text, metadata, images, and structured data like infoboxes, making it easier to transform unstructured web content into structured, analyzable data. Indexify allows you to choose between different extractors based on your use case and source of data. If you want to learn more about extractors, their design and usage, read the Indexify [documentation](https://docs.getindexify.ai/concepts/).

| Extractor Name | Use Case | Supported Input Types |
|----------------|----------|------------------------|
| Wikipedia | Extract content from Wikipedia pages | text/html, text/plain |

## [Wikipedia](https://github.com/tensorlakeai/indexify-extractors/tree/main/html/wikipedia)

### Description
Extract text content from Wikipedia HTML pages. This extractor can parse various elements of a Wikipedia page, including the title, sections, infobox, and images.

### Input Data Types
```
["text/html", "text/plain"]
```

### Class Name
```
WikipediaExtractor
```

### Download Command
=== "Bash"
```bash
indexify-extractor download tensorlake/wikipedia
```

=== "Docker"
```bash
docker pull tensorlake/wikipedia
```
