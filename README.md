<p align="center">
  <img src="docs/images/logo/TL-Dark.svg" alt="Indexify Logo" width="400"/>
</p>

<h1 align="center">Indexify</h1>

<p align="center">
  <strong>Compute Engine for Building Data Platforms</strong>
</p>

<p align="center">
  <a href="https://github.com/tensorlakeai/indexify/releases"><img src="https://img.shields.io/github/v/release/tensorlakeai/indexify?style=flat-square&color=blue" alt="Release"></a>
  <a href="https://github.com/tensorlakeai/indexify/blob/main/LICENSE"><img src="https://img.shields.io/github/license/tensorlakeai/indexify?style=flat-square" alt="License"></a>
  <a href="https://join.slack.com/t/tensorlakecloud/shared_invite/zt-32fq4nmib-gO0OM5RIar3zLOBm~ZGqKg"><img src="https://img.shields.io/badge/slack-TensorlakeCloud-7C3AED?style=flat-square&logo=slack" alt="Slack"></a>
  <a href="https://docs.tensorlake.ai"><img src="https://img.shields.io/badge/docs-tensorlake.ai-0EA5E9?style=flat-square" alt="Documentation"></a>
</p>

---

Indexify is a compute engine for building **data platforms** in Python. Create large-scale data processing workflows and agentic applications with durable execution—functions automatically retry on failure, and workflows seamlessly scale across machines. Deploy your applications as live API endpoints for seamless integration with existing systems.

> **Note:** Indexify is the open-source core that powers [Tensorlake Cloud](https://tensorlake.ai)—a serverless platform for document processing, media pipelines, and agentic applications.

## ✨ Features

| Feature | Description |
|---------|-------------|
| **🐍 Python Native** | Define workflows as Python functions with type hints—no DSLs, YAML, or config files |
| **🔄 Durable Execution** | Functions automatically retry on failure with persistent state across restarts |
| **📊 Distributed Map/Reduce** | Parallelize functions over sequences across machines with automatic data shuffling |
| **🔀 Dynamic Routing** | Router functions dynamically choose execution paths based on inputs |
| **🤖 Local Inference** | Run LLMs using LlamaCPP, vLLM, or Hugging Face Transformers directly in functions |
| **⚡ Request Queuing** | Automatically queue and batch invocations to maximize GPU utilization |
| **🌐 Multi-Cloud** | Run across multiple clouds, datacenters, or regions with minimal configuration |
| **📈 Scale to Zero** | Queue requests when no executors are running—process when they come online |

## 🚀 What Can You Build?

### Large-Scale Data Processing Workflows
Build production-grade data pipelines entirely in Python with automatic parallelization, fault tolerance, and distributed execution:

- **Document Processing** — Extract tables, images, and text from PDFs at scale; build knowledge graphs; implement RAG pipelines
- **Media Pipelines** — Transcribe and summarize video/audio content; detect and describe objects in images
- **ETL & Data Transformation** — Process millions of records with distributed map/reduce operations

### Agentic Applications
Build durable AI agents that reliably execute multi-step workflows:

- **Tool-Calling Agents** — Orchestrate LLM tool calls with automatic state management and retry logic
- **Multi-Agent Systems** — Coordinate multiple agents with durable message passing
- **Human-in-the-Loop** — Build approval workflows with persistent state across sessions

📖 **[Explore the Cookbooks →](https://github.com/tensorlakeai/cookbooks)** for complete examples and tutorials.

## 📦 Installation

Using pip:
```bash
pip install indexify tensorlake
```

## 🎯 Quick Start

### Define Your Application

Create applications using `@application()` and `@function()` decorators. Each function runs in its own isolated sandbox with durable execution—if a function crashes, it automatically restarts from where it left off.

```python
from typing import List
from pydantic import BaseModel
from tensorlake.applications import application, function, Image, run_local_application

# Define container image with dependencies
embedding_image = Image(base_image="python:3.11-slim", name="embedding_image").run(
    "pip install sentence-transformers langchain-text-splitters chromadb"
)


class TextChunk(BaseModel):
    chunk: str
    page_number: int


class ChunkEmbedding(BaseModel):
    text: str
    embedding: List[float]


@function(image=embedding_image)
def chunk_text(text: str) -> List[TextChunk]:
    """Split text into chunks for embedding."""
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=20)
    texts = splitter.create_documents([text])
    return [
        TextChunk(chunk=chunk.page_content, page_number=i)
        for i, chunk in enumerate(texts)
    ]


@function(image=embedding_image)
def embed_chunks(chunks: List[TextChunk]) -> List[ChunkEmbedding]:
    """Embed text chunks using sentence transformers."""
    from sentence_transformers import SentenceTransformer
    
    model = SentenceTransformer("all-MiniLM-L6-v2")
    return [
        ChunkEmbedding(text=chunk.chunk, embedding=model.encode(chunk.chunk).tolist())
        for chunk in chunks
    ]


@function(image=embedding_image)
def write_to_vectordb(embeddings: List[ChunkEmbedding]) -> str:
    """Write embeddings to ChromaDB."""
    import chromadb
    import uuid
    
    client = chromadb.PersistentClient("./chromadb_data")
    collection = client.get_or_create_collection("documents")
    
    for emb in embeddings:
        collection.upsert(
            ids=[str(uuid.uuid4())],
            embeddings=[emb.embedding],
            documents=[emb.text],
        )
    return f"Indexed {len(embeddings)} chunks"


@application()
@function(description="Text embedding pipeline")
def text_embedder(text: str) -> str:
    """Main application: chunks text, embeds it, and stores in vector DB."""
    chunks = chunk_text(text)
    embeddings = embed_chunks(chunks)
    result = write_to_vectordb(embeddings)
    return result
```

### Deploy to Tensorlake Cloud (Fastest Way to Get Started)

[Tensorlake Cloud](https://cloud.tensorlake.ai) is the fastest way to test and deploy your applications—no infrastructure setup required. Get an API key and deploy in seconds:

```bash
# Set your API key
export TENSORLAKE_API_KEY="your-api-key"

# Deploy the application
tensorlake deploy workflow.py
```

```python
from tensorlake.applications import run_remote_application

request = run_remote_application(text_embedder, "Your document text here...")
result = request.output()
print(result)
```

### Self-Host with Indexify

If you prefer to self-host or need on-premise deployment, you can run the Indexify server locally:

```bash
# Terminal 1: Start the server
docker run -p 8900:8900 tensorlake/indexify-server

# Terminal 2: Start an executor (repeat for more parallelism)
indexify-cli executor
```

Set the API URL and deploy:

```bash
export TENSORLAKE_API_URL=http://localhost:8900
tensorlake deploy workflow.py
```

Run your application:

```python
from tensorlake.applications import run_remote_application

request = run_remote_application(text_embedder, "Your document text here...")
result = request.output()
print(result)
```

### Test Locally (No Server Required)

For quick iteration during development, run applications locally without any infrastructure:

```python
if __name__ == "__main__":
    request = run_local_application(text_embedder, "Your document text here...")
    result = request.output()
    print(result)
```

## 🏗️ Production Self-Hosted Deployment

For production self-hosted deployments, see [operations/k8s](operations/k8s/) for Kubernetes deployment manifests and Helm charts.

## ☁️ Tensorlake Cloud vs Self-Hosted Indexify

**Start with Tensorlake Cloud** to build and test your applications without infrastructure overhead. When you're ready for self-hosting or need on-premise deployment, Indexify provides the same runtime you can run anywhere.

| Feature | Tensorlake Cloud | Indexify (Self-Hosted) |
|---------|------------------|------------------------|
| **Setup Time** | Instant—just get an API key | Deploy server + executors |
| **Image Building** | Automatic image builds when you deploy | Build and manage container images yourself |
| **Auto-Scaling** | Dynamic container scaling with scale-to-zero | Manual executor management |
| **Security** | Secure sandboxes (gVisor, Linux containers, virtualization) | Standard container isolation |
| **Secrets** | Built-in secret management for applications | Manage secrets externally |
| **Observability** | Logging, tracing, and observability built-in | Bring your own logging/tracing |
| **Testing** | Interactive playground to invoke applications | Local development only |

[Get started with Tensorlake Cloud →](https://cloud.tensorlake.ai)

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

Indexify is licensed under the [Apache 2.0 License](LICENSE).

---

<p align="center">
  <a href="https://tensorlake.ai">Website</a> •
  <a href="https://docs.tensorlake.ai">Docs</a> •
  <a href="https://join.slack.com/t/tensorlakecloud/shared_invite/zt-32fq4nmib-gO0OM5RIar3zLOBm~ZGqKg">Slack</a> •
  <a href="https://twitter.com/tensoraboratory">Twitter</a>
</p>
