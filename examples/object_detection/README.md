# Object Detection and Description Pipeline

This project is a pipeline for object detection and description. It uses Ultralytics YOLOv8 to detect objects in images. Visual Description is generated using a pre-trained moondream model.

## How It Works

The pipeline has two compute classes:

1. Object Detection
2. Visual Description

The output of the object detection is a list of bounding boxes and the class of the object. The original image and the result of the object detection are passed to the Visual Description model. The output of the Visual Description model has the description and the bounding boxes detected.

## How to Run 

### Locally on your Laptop

1. Start the server and an image with the dependencies of the functions.

This example works only on GPU machines.

```bash
docker compose up
```

2. Run the Workflow
```python
python workflow.py
```

Here is the output:
```
[ImageDescription(description='The image captures a bustling street scene in Times Square, New York, teeming with yellow taxis and surrounded by a vibrant array of billboards and advertisements.', detections=[Detection(bbox=[588.925048828125, 468.69464111328125, 796.9473876953125, 619.639404296875], label='car', confidence=0.8861740827560425), Detection(bbox=[319.2535095214844, 480.70361328125, 454.46826171875, 559.7138671875], label='car', confidence=0.836341142654419), Detection(bbox=[746.5311889648438, 475.47247314453125, 918.6951293945312, 579.5167236328125], label='car', confidence=0.7883055806159973), Detection(bbox=[72.3926010131836, 517.8421630859375, 144.1722412109375, 592.4739990234375], label='potted plant', confidence=0.7109927535057068), Detection(bbox=[545.43994140625, 468.8044738769531, 593.8861083984375, 500.2347106933594], label='car', confidence=0.708862841129303), Detection(bbox=[907.5588989257812, 469.9573059082031, 924.8134765625, 513.6497192382812], label='person', confidence=0.4035480320453644), Detection(bbox=[148.98741149902344, 470.06207275390625, 197.73593139648438, 532.3275756835938], label='potted plant', confidence=0.30130401253700256), Detection(bbox=[519.849853515625, 471.2547912597656, 548.244140625, 497.1222229003906], label='car', confidence=0.29631689190864563), Detection(bbox=[778.8235473632812, 462.0633850097656, 826.0460205078125, 485.4345703125], label='car', confidence=0.28815868496894836), Detection(bbox=[967.5180053710938, 462.99041748046875, 979.6304321289062, 526.1993408203125], label='person', confidence=0.26838958263397217)])]
```


