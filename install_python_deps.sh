#!/bin/sh

pip install torch --index-url https://download.pytorch.org/whl/cpu
pip install transformers[torch] optimum[onnxruntime] onnx onnxruntime