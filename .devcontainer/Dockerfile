FROM ubuntu
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt -y install software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt-get update

RUN apt update

RUN apt -y install build-essential
RUN apt -y install make cmake
RUN apt -y install g++
RUN apt -y install sqlite3
RUN apt -y install python3.11
RUN alias python=python3.11
RUN apt -y install pip
# RUN apt -y install virtualenv
RUN apt -y install python3.11-venv
RUN apt -y install protobuf-compiler protobuf-compiler-grpc
RUN apt -y install ca-certificates curl gnupg pkg-config libssl-dev vim
RUN apt -y install docker-compose
# RUN ln -s /usr/bin/docker-compose /compose/docker-compose
RUN apt -y install python3.11-dev

RUN export OPENSSL_LIB_DIR="/usr/lib/ssl"
RUN export OPENSSL_INCLUDE_DIR="/usr/include/openssl"
RUN export OPENSSL_LIB_DIR="/usr/lib/aarch64-linux-gnu/"
RUN export OTEL_TRACES_SAMPLER=always_on

# RUN python3.11 -m venv ve
# RUN source ve/bin/activate

# WORKDIR /workspace/indexify/
# RUN python3.11 -m pip install .
# RUN (cd sdk-py && pip install -e .)

# RUN export PATH="/usr/bin/:$PATH"
# RUN export PYTHONPATH=${PYTHONPATH}:${PWD}/ve/lib/python3.11/site-packages


RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y
