from indexify import Image

http_client_image = (
    Image()
    .name("tensorlake/pdf-blueprint-download")
    .base_image(f"python:3.11-slim-bookworm")
    .run("pip install httpx")
)

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
    .base_image("pytorch/pytorch:2.4.1-cuda11.8-cudnn9-runtime")
    .run("pip install sentence-transformers")
    .run("pip install langchain")
    .run("pip install pillow")
    .run("pip install py-inkwell")
    .run("pip install opentelemetry-api")
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
    .base_image("pytorch/pytorch:2.4.1-cuda11.8-cudnn9-runtime")
    .run("apt update")
    .run("apt install -y libgl1-mesa-glx git g++")
    .run("pip install git+https://github.com/facebookresearch/detectron2.git@v0.6")
    .run("apt install -y tesseract-ocr")
    .run("apt install -y libtesseract-dev")
    .run('pip install "py-inkwell[inference]"')
    .run('pip install docling')
    .run("pip install elastic-transport")
)
