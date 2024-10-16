import urllib.request
from indexify import RemoteGraph, Graph, Image
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import IndexifyFunction, indexify_function
from pydantic import BaseModel
from typing import List
import io


image = Image().name("tensorlake/blueprints-ultralytics").run("pip install ultralytics").run("pip install transformers").run("pip install einops")

class Detection(BaseModel):
    bbox: List[float]
    label: str
    confidence: float

class ObjectDetectionResult(BaseModel):
    detections: List[Detection]
    image: File

class ObjectDetector(IndexifyFunction):
    name = "object_detector"
    image = image

    def __init__(self):
        super().__init__()
        from ultralytics import YOLO
        self.model = YOLO("yolov8n.pt")

    def run(self, img: File) -> ObjectDetectionResult:
        import cv2
        import numpy as np
        nparr = np.frombuffer(img.data, np.uint8)
        image_arr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        # Run inference
        results = self.model(image_arr, conf=0.25, iou=0.7)
        detections = []

        for result in results:
            boxes = result.boxes
            for box in boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                class_id = int(box.cls)
                class_name = result.names[class_id]
                confidence = float(box.conf)
                detections.append(Detection(bbox=[x1, y1, x2, y2], label=class_name, confidence=confidence))

        return ObjectDetectionResult(detections=detections, image=img)
    

class ImageDescription(BaseModel):
    description: str
    detections: List[Detection]

class FilteredImage(BaseModel):
    is_filtered: bool

class ImageDescriber(IndexifyFunction):
    name = "image_describer"
    def __init__(self):
        super().__init__()
        from transformers import AutoModelForCausalLM, AutoTokenizer
        model_id = "vikhyatk/moondream2"
        revision = "2024-08-26"
        self.model = AutoModelForCausalLM.from_pretrained(
            model_id, trust_remote_code=True, revision=revision
        )
        self.tokenizer = AutoTokenizer.from_pretrained(model_id, revision=revision)

    def run (self, detection_result: ObjectDetectionResult) -> ImageDescription:
        from PIL import Image
        image = Image.open(io.BytesIO(detection_result.image.data))
        enc_image = self.model.encode_image(image)
        result = self.model.answer_question(enc_image, "Describe this image.", self.tokenizer)
        return ImageDescription(description=result, detections=detection_result.detections)


if __name__=="__main__":
    from pathlib import Path
    import urllib.request
    with urllib.request.urlopen('https://www.frommers.com/system/media_items/attachments/000/868/461/s980/Frommers-New-York-City-Getting-Around-1190x768.webp?1647177178') as response:
        data = response.read()
        img = File(data=data)
    img = File(data=data)

    g = Graph(name="object_detection_workflow", start_node=ObjectDetector)
    g.add_edge(ObjectDetector, ImageDescriber)
    invocation_id = g.run(block_until_done=True, img=img)
    print(g.output(invocation_id, "image_describer"))

    # g = RemoteGraph.deploy(g, server_url="http://100.106.216.46:8900")
    # invocation_id = g.run(block_until_done=True, img=img)
    # output = g.output(invocation_id, "object_detector")
    # print(output)