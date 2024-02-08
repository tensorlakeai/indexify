FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update
RUN apt -y install software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt -y remove python3.10

RUN apt -y install build-essential make sudo cmake g++ sqlite3 python3.11 python3.11-distutils python3.11-venv python3.11-dev protobuf-compiler protobuf-compiler-grpc ca-certificates curl gnupg pkg-config libssl-dev vim docker-compose libclang-dev librocksdb-dev

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y
# rustfmt nightly is used for formatting
RUN /root/.cargo/bin/rustup toolchain install nightly

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
RUN export NVM_DIR="$([ -z "${XDG_CONFIG_HOME-}" ] && printf %s "${HOME}/.nvm" || printf %s "${XDG_CONFIG_HOME}/nvm")"
RUN [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" && nvm install stable

RUN apt install postgresql
RUN service postgresql start
RUN sudo -u postgres psql -c "CREATE DATABASE indexify";

RUN echo 'alias python=python3.11' >> ~/.bashrc
RUN echo 'export PYTHON=python3.11' >> ~/.bashrc
RUN echo 'export PYO3_CROSS_PYTHON_VERSION=3.11' >> ~/.bashrc
RUN echo 'export PYTHONPATH=${PYTHONPATH}:/workspaces/indexify/extractors' >> ~/.bashrc
