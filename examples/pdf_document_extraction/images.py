from tensorlake import Image

chroma_image = (
    Image()
    .name("tensorlake/blueprints-chromadb")
    .base_image(f"python:3.11-slim-bookworm")
    .run("pip install chromadb")
    .run("pip install pillow")
)

st_image = (
    Image()
    .name("tensorlake/pdf-blueprint-st")
    .base_image("pytorch/pytorch:2.4.1-cuda11.8-cudnn9-devel")
    .run("pip install sentence-transformers")
    .run("pip install langchain")
    .run("pip install pillow")
    .run("pip install opentelemetry-api")
    .run("pip install elasticsearch")
    .run("pip install elastic-transport")
)

lance_image = (
    Image()
    .name("tensorlake/pdf-blueprint-lancdb")
    .base_image(f"python:3.11-slim-bookworm")
    .run("pip install lancedb")
)

inkwell_image_gpu = (
    Image()
    .name("tensorlake/pdf-blueprint-pdf-parser-gpu")
    .base_image("pytorch/pytorch:2.5.1-cuda12.4-cudnn9-devel")
    .run("apt update")
    .run("apt install -y libgl1-mesa-glx")
    .run('pip install docling')
    .run('pip install torch==2.5.1 torchvision==0.2.1')
)
