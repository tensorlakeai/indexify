from indexify import Image


tf_image = (
    Image()
    .base_image("pytorch/pytorch:2.4.1-cuda11.8-cudnn9-runtime")
    .name("tensorlake/common-torch-deps-indexify-executor")
    .run("pip install transformers")
    .run("pip install sentence_transformers")
    .run("pip install langchain")
)
