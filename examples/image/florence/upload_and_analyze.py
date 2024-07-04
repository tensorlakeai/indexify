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
        content_id=content_id,
        graph_name="florence_image_analyzer",
        policy_name="detailed_caption"
    )
    
    object_detection = client.get_extracted_content(
        content_id=content_id,
        graph_name="florence_image_analyzer",
        policy_name="object_detection"
    )
    
    referring_expression = client.get_extracted_content(
        content_id=content_id,
        graph_name="florence_image_analyzer",
        policy_name="referring_expression_segmentation"
    )
    
    return detailed_caption, object_detection, referring_expression

# Example usage
if __name__ == "__main__":
    image_url = "https://huggingface.co/datasets/huggingface/documentation-images/resolve/main/transformers/tasks/car.jpg?download=true"
    image_path = "sample_image.jpg"
    
    # Download the image
    download_image(image_url, image_path)
    
    # Analyze the image
    caption, objects, segmentation = analyze_image(image_path)
    
    print("Detailed Caption:")
    print(caption[0]['content'].decode('utf-8'))
    
    print("\nObject Detection:")
    print(objects[0]['content'].decode('utf-8'))
    
    print("\nReferring Expression Segmentation:")
    print(segmentation[0]['content'].decode('utf-8'))