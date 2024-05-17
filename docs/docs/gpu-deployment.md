# Running Extractor on GPU

For local development of your extractor, you can definitely use your CPU. However, when you are ready to deploy your extractor to be used for production, you should consider running it on a GPU. This is because the GPU is much faster than the CPU and can handle more workloads.

To run your extractor on a GPU, you need to have a GPU-enabled machine. You can either use a cloud-based service like AWS, Google Cloud, or Azure, or you can use your own machine as long as it has a GPU.

In this guide, we will show you how to run your extractor on a AWS EC2 instance running on Ubuntu. We will skip the steps on how to spin up an EC2 instance as there are already many guides available online on how to do that.

## Step 1: Install NVIDIA Drivers

First of all, try running the command below to see if you have NVIDIA drivers installed:

```bash
nvidia-smi
```

If you are able to see the NVIDIA driver version, then you are good to go and you can skip this step. If not, you need to install the NVIDIA drivers by running following this guide: [Installing NVIDIA Drivers on Ubuntu](https://ubuntu.com/server/docs/nvidia-drivers-installation).

You might want to choose the driver dedicated for a server instead of the desktop version. After installing the drivers, you might need to reboot your machine.

## Step 2: Install NVIDIA Container Toolkit

Next, you need to install the NVIDIA Container Toolkit. This is required to run Docker containers on your GPU. You can install it by following this guide: [Installing NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html).

Make sure to follow the guide to configure the toolkit with Docker.

## Step 3: Run Your Extractor

If you are running a custom-built extractor, you can build a Docker image for your GPU-enabled extractor by following the [Extractor Development Guide](./apis/develop_extractors.md).

If you are using a pre-built GPU-accelerated Indexify extractor like `tensorlake/moondream`, you can run it by specifying the `--gpus` flag:

```sh
docker run \
    --volume /tmp:/tmp \
    --gpus all \
    tensorlake/moondream \
    join-server \
    --coordinator-addr=$ADDRESS:8950 \
    --ingestion-addr=$ADDRESS:8900
```
