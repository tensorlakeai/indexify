# Image Analysis with Indexify and Florence Extractor

We demonstrate how to create a pipeline capable of performing multiple image analysis tasks, including detailed captioning, object detection, and referring expression segmentation.

The pipeline will consist of three main tasks:

1. Detailed Image Captioning using the `<MORE_DETAILED_CAPTION>` task.
2. Object Detection using the `<OD>` task.
3. Referring Expression Segmentation using the `<REFERRING_EXPRESSION_SEGMENTATION>` task.

## Prerequisites

Before we begin, ensure you have the following:

- Create a virtual env with Python 3.9 or later
  ```shell
  python3.9 -m venv ve
  source ve/bin/activate
  ```
- `pip` (Python package manager)
- Basic familiarity with Python and command-line interfaces

## Setup

### Install Indexify

First, let's install Indexify using the official installation script & start the server:

```bash
curl https://getindexify.ai | sh
./indexify server -d
```
This starts a long-running server that exposes ingestion and retrieval APIs to applications.

### Install Required Extractor

Next, we'll install the necessary extractor in a new terminal and start it:

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/florence
indexify-extractor join-server
```

## Creating the Extraction Graph

The extraction graph defines the flow of data through our image analysis pipeline. We'll create a graph that performs three tasks on the input image.

Create a new Python file called `florence_analysis_graph.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'florence_image_analyzer'
extraction_policies:
  - extractor: 'tensorlake/florence'
    name: 'detailed_caption'
    input_params:
      task_prompt: '<MORE_DETAILED_CAPTION>'
  - extractor: 'tensorlake/florence'
    name: 'object_detection'
    input_params:
      task_prompt: '<OD>'
  - extractor: 'tensorlake/florence'
    name: 'referring_expression_segmentation'
    input_params:
      task_prompt: '<REFERRING_EXPRESSION_SEGMENTATION>'
      text_input: 'a green car'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

You can run this script to set up the pipeline:
```bash
python florence_analysis_graph.py
```

## Implementing the Image Analysis Pipeline

Now that we have our extraction graph set up, we can upload images and make the pipeline analyze them. Create a file `upload_and_analyze.py`:

```python
import requests
from indexify import IndexifyClient

def download_image(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"Image downloaded and saved to {save_path}")

def analyze_image(image_path):
    client = IndexifyClient()
    
    # Upload the image file
    content_id = client.upload_file("florence_image_analyzer", image_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the analysis results
    detailed_caption = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="florence_image_analyzer",
        policy_name="detailed_caption"
    )
    
    object_detection = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="florence_image_analyzer",
        policy_name="object_detection"
    )
    
    referring_expression = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="florence_image_analyzer",
        policy_name="referring_expression_segmentation"
    )
    
    return detailed_caption, object_detection, referring_expression

# Example usage
if __name__ == "__main__":
    image_urls = [
        "https://www.greencarguide.co.uk/wp-content/uploads/2023/02/Skoda-Enyaq-iV-vRS-001-low-res-600x400.jpeg",
        "https://huggingface.co/datasets/huggingface/documentation-images/resolve/main/transformers/tasks/car.jpg?download=true"
    ]
    
    for i,image_url in enumerate(image_urls, 1):
        image_path = "sample_image.jpg"
        
        # Download the image
        download_image(image_url, image_path)
        
        # Analyze the image
        caption, objects, segmentation = analyze_image(image_path)
        
        print("Detailed Caption:")
        print(caption[0]['content'].decode('utf-8'))
        
        print("\nObject Detection:")
        print(objects[0]['content'].decode('utf-8'))
```

You can run the Python script to process an image and generate analysis results:
```bash
python upload_and_analyze.py
```

## Results

With the helper functions to visualize the output images in [`plot.py`](https://github.com/tensorlakeai/indexify/blob/main/examples/image/florence/plot.py) we can get results like this:
```python
from PIL import Image
from plot import plot_bbox, draw_polygons
image = Image.open(requests.get(image_url, stream=True).raw)
```
```python
plot_bbox(image, objects[0]['content'].decode('utf-8'), output_filename=f'bbox_output_{i}.png')
```
![](https://docs.getindexify.ai/example_code/image/florence/detect.png)
```python
draw_polygons(image, segmentation[0]['content'].decode('utf-8'), fill_mask=True, output_filename=f'polygon_output_{i}.png')
```
![](https://docs.getindexify.ai/example_code/image/florence/segment.png)

## Customization and Advanced Usage

You can customize the analysis process by modifying the `input_params` in the extraction graph. For example:

- To add additional tasks or modify existing ones:
  ```yaml
  extraction_policies:
    - extractor: 'tensorlake/florence'
      name: 'optical_character_recognition'
      input_params:
        task_prompt: '<OCR>'
  ```

- To run pre-defined tasks that requires additional inputs:
  ```yaml
  input_params:
    task_prompt: '<CAPTION_TO_PHRASE_GROUNDING>'
    text_input: 'A green car parked in front of a yellow building.'
  ```

Experiment with different parameters and tasks to tailor the analysis to your specific use case.

## Conclusion

This example showcases the versatility of using Indexify with the Florence extractor for image analysis:

1. **Multitask Capability**: The Florence extractor can perform various image analysis tasks within a single pipeline.
2. **Scalability**: Indexify server can be deployed on a cloud and process numerous images uploaded into it.
3. **Flexibility**: You can easily add or modify tasks to suit your specific needs.
4. **Integration**: The analysis results can be easily integrated into downstream tasks such as image search, content moderation, or further processing.

## Next Steps

- Explore more tasks available in the [Florence extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/image/florence)
- Learn how to use the analysis results in applications like image search or content filtering
- Dive deeper into Indexify's capabilities at https://docs.getindexify.ai
