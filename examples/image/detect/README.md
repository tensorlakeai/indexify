# YOLO Image Object Detection with Indexify

We demonstrate how to create a pipeline capable of ingesting image files and detecting objects within them using the YOLO (You Only Look Once) model.

The pipeline will use the `tensorlake/yolo-extractor` extractor to process images and identify objects within them, providing bounding boxes, class names, and confidence scores for each detected object.

## Prerequisites

Before starting, ensure you have:

- A virtual environment with Python 3.9 or later
  ```shell
  python3.9 -m venv ve
  source ve/bin/activate
  ```
- `pip` (Python package manager)
- Basic familiarity with Python and command-line interfaces

## Setup

### Install Indexify

First, install Indexify using the official installation script & start the server:

```bash
curl https://getindexify.ai | sh
./indexify server -d
```

This starts a long-running server that exposes ingestion and retrieval APIs to applications.

### Install Required Extractor

Next, install the YOLO extractor in a new terminal and start it:

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/yolo-extractor
indexify-extractor join-server
```

## Creating the Extraction Graph

The extraction graph defines the flow of data through our object detection pipeline. Create a new Python file called `yolo_detection_graph.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'yolo_detector'
extraction_policies:
  - extractor: 'tensorlake/yolo-extractor'
    name: 'image_object_detection'
    input_params:
      model_name: 'yolov8n.pt'
      conf: 0.25
      iou: 0.7
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Run this script to set up the pipeline:
```bash
python yolo_detection_graph.py
```

## Implementing the Object Detection Pipeline

Now that we have our extraction graph set up, we can upload images and make the pipeline detect objects. Create a file `upload_and_detect.py`:

```python
import os
import requests
from indexify import IndexifyClient

def download_image(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"Image downloaded and saved to {save_path}")

def detect_objects(image_path):
    client = IndexifyClient()
    
    # Upload the image file
    content_id = client.upload_file("yolo_detector", image_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the detected objects
    detections = client.get_extracted_content(
        content_id=content_id,
        graph_name="yolo_detector",
        policy_name="image_object_detection"
    )
    
    return [{'bbox': det['bbox'], 'class': det['class'], 'confidence': det['features'][0]['metadata']['score']} for det in detections]

# Example usage
if __name__ == "__main__":
    image_url = "https://example.com/path/to/image.jpg"
    image_path = "sample_image.jpg"
    
    # Download the image
    download_image(image_url, image_path)
    
    # Detect objects in the image
    objects = detect_objects(image_path)
    print(f"Number of objects detected: {len(objects)}")
    print("\nDetected objects:")
    for obj in objects:
        print(f"Class: {obj['class']}, Confidence: {obj['confidence']:.2f}, Bounding Box: {obj['bbox']}")
```

## Running the Object Detection Process

You can run the Python script to process an image and detect objects:
```bash
python upload_and_detect.py
```

## Customization and Advanced Usage

You can customize the object detection process by modifying the `input_params` in the extraction graph. For example:

- To change the model or detection thresholds:
  ```yaml
  input_params:
    model_name: 'yolov8m.pt'
    conf: 0.3
    iou: 0.5
  ```

- To use a custom-trained YOLO model:
  ```yaml
  input_params:
    model_name: '/path/to/your/custom_model.pt'
    conf: 0.25
    iou: 0.7
  ```

Experiment with different parameters to find the best balance between detection accuracy and speed for your specific use case.

## Conclusion

This example demonstrates the power and flexibility of using Indexify for image object detection:

1. **Scalability**: Indexify server can be deployed on a cloud and process numerous images uploaded into it. If any step in the pipeline fails, it automatically retries on another machine.
2. **Flexibility**: You can easily swap out components or adjust parameters to suit your specific needs.
3. **Integration**: The detected objects can be easily integrated into downstream tasks such as image analysis, indexing, or further processing.

## Next Steps

- Learn more about Indexify on our docs - https://docs.getindexify.ai
- Explore how to use the detected objects for tasks like image search or visual question-answering.