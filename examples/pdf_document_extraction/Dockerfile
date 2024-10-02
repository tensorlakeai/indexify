# Use an official Python 3.10 image from Docker Hub
FROM python:3.10-bookworm

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file into the container at /app
COPY requirements.txt .

RUN apt-get update
RUN apt install -y libgl1-mesa-glx

RUN pip install --no-cache-dir py-inkwell

# Install the required Python packages

RUN pip install git+https://github.com/facebookresearch/detectron2.git@v0.6

RUN pip install -r requirements.txt

RUN apt install tesseract-ocr -y
RUN apt install libtesseract-dev -y

# Copy the entire application (including the script) to the container
COPY workflow.py .

# Define the command to run your Python script with an argument
# You can override the argument when running the container
ENTRYPOINT ["python", "workflow.py"]