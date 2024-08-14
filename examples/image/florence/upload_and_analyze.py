import requests
from PIL import Image
from indexify import IndexifyClient
from plot import plot_bbox, draw_polygons

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

        image = Image.open(requests.get(image_url, stream=True).raw)
        plot_bbox(image, objects[0]['content'].decode('utf-8'), output_filename=f'bbox_output_{i}.png')
        draw_polygons(image, segmentation[0]['content'].decode('utf-8'), fill_mask=True, output_filename=f'polygon_output_{i}.png')