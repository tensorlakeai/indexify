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