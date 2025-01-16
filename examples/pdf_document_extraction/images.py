from tensorlake import Image

http_client_image = (
    Image()
    .name("tensorlake/pdf-blueprint-download")
    .run("pip install httpx")
)

chroma_image = (
    Image()
    .name("tensorlake/blueprints-chromadb")
    .run("pip install chromadb")
    .run("pip install pillow")
)

st_image = (
    Image()
    .name("tensorlake/pdf-blueprint-st")
    .run("pip install sentence-transformers")
    .run("pip install langchain")
    .run("pip install pillow")
    .run("pip install py-inkwell")
    .run("pip install opentelemetry-api")
    .run("pip install elastic-transport")
)

lance_image = (
    Image()
    .name("tensorlake/pdf-blueprint-lancdb")
    .run("pip install lancedb")
)

parser_image = (
    Image()
    .name("tensorlake/pdf-blueprint-pdf-parser")
    .run("apt update")
    .run("apt install -y libgl1-mesa-glx git g++")
    .run("pip install torch torchaudio torchvideo")
    .run("pip install git+https://github.com/facebookresearch/detectron2.git@v0.6")
    .run("apt install -y tesseract-ocr")
    .run("apt install -y libtesseract-dev")
    .run('pip install "py-inkwell[inference]"')
    .run('pip install docling')
    .run("pip install elastic-transport")
)


# parser_image = (
#     Image()
#     .name("tensorlake/pdf-blueprint-pdf-parser")
#     .base_image("pytorch/pytorch:2.4.1-cuda11.8-cudnn9-runtime")
#     .run("apt update")
#     .run("apt install -y libgl1-mesa-glx git g++")
#     .run("pip install git+https://github.com/facebookresearch/detectron2.git@v0.6")
#     .run("apt install -y tesseract-ocr")
#     .run("apt install -y libtesseract-dev")
#     .run('pip install "py-inkwell[inference]"')
#     .run('pip install docling')
#     .run("pip install elastic-transport")
# )
