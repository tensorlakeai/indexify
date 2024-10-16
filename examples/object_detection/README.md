# Object Detection and Description Pipeline

This project is a pipeline for object detection and description. It uses Ultralytics YOLOv8 to detect objects in images. Visual Description is generated using a pre-trained moondream model.

## How It Works

The pipeline has two compute classes:

1. Object Detection
2. Visual Description

The output of the object detection is a list of bounding boxes and the class of the object. The original image and the result of the object detection are passed to the Visual Description model. The output of the Visual Description model has the description and the bounding boxes detected.

## How to Run 

### Locally on your Laptop

This example works only on GPU machines.

```bash
docker compose up
```

```python
python workflow.py
```


