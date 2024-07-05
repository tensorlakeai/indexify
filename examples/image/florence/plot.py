import ast
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from PIL import Image, ImageDraw, ImageFont
import random
import numpy as np
import io

colormap = ['blue','orange','green','purple','brown','pink','gray','olive','cyan','red',
            'lime','indigo','violet','aqua','magenta','coral','gold','tan','skyblue']

def plot_bbox(image, data, output_filename='bbox_output.png'):
    data = (ast.literal_eval(data))['<OD>']
    # Create a figure and axes
    fig, ax = plt.subplots()
    
    # Display the image
    ax.imshow(image)
    
    # Plot each bounding box
    for bbox, label in zip(data['bboxes'], data['labels']):
        # Unpack the bounding box coordinates
        x1, y1, x2, y2 = bbox
        # Create a Rectangle patch
        rect = patches.Rectangle((x1, y1), x2-x1, y2-y1, linewidth=1, edgecolor='r', facecolor='none')
        # Add the rectangle to the Axes
        ax.add_patch(rect)
        # Annotate the label
        plt.text(x1, y1, label, color='white', fontsize=8, bbox=dict(facecolor='red', alpha=0.5))
    
    # Remove the axis ticks and labels
    ax.axis('off')
    
    # Save the plot
    plt.savefig(output_filename, bbox_inches='tight', pad_inches=0)
    print(f"Bounding box image saved as {output_filename}")

def draw_polygons(image, prediction, fill_mask=False, output_filename='polygon_output.png'):
    prediction = (ast.literal_eval(prediction))['<REFERRING_EXPRESSION_SEGMENTATION>']
    draw = ImageDraw.Draw(image)
    
    # Set up scale factor if needed (use 1 if not scaling)
    scale = 1
    
    # Iterate over polygons and labels
    for polygons, label in zip(prediction['polygons'], prediction['labels']):
        color = random.choice(colormap)
        fill_color = random.choice(colormap) if fill_mask else None
        
        for _polygon in polygons:
            _polygon = np.array(_polygon).reshape(-1, 2)
            if len(_polygon) < 3:
                print('Invalid polygon:', _polygon)
                continue
            
            _polygon = (_polygon * scale).reshape(-1).tolist()
            
            # Draw the polygon
            if fill_mask:
                draw.polygon(_polygon, outline=color, fill=fill_color)
            else:
                draw.polygon(_polygon, outline=color)
            
            # Draw the label text
            draw.text((_polygon[0] + 8, _polygon[1] + 2), label, fill=color)

    # Save the image
    image.save(output_filename)
    print(f"Polygon image saved as {output_filename}")