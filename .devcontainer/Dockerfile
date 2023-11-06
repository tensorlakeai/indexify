FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt -y install software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt-get update

RUN apt update

RUN apt -y install build-essential
RUN apt -y remove python3.10
RUN apt -y install make cmake
RUN apt -y install g++
RUN apt -y install sqlite3
RUN apt -y install python3.11 python3.11-distutils
RUN apt -y install python3.11-venv
RUN apt -y install python3.11-dev
RUN apt -y install protobuf-compiler protobuf-compiler-grpc
RUN apt -y install ca-certificates curl gnupg pkg-config libssl-dev vim
RUN apt -y install docker-compose

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y

RUN echo 'alias python=python3.11' >> ~/.bashrc
RUN echo 'export PYTHON=python3.11' >> ~/.bashrc
RUN echo 'export PYO3_CROSS_PYTHON_VERSION=3.11' >> ~/.bashrc
RUN echo 'export PYTHONPATH=${PYTHONPATH}:/workspaces/indexify/extractors' >> ~/.bashrc