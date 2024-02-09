FROM ubuntu:22.04
LABEL stage=builder

WORKDIR /indexify-build
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt -y install software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt install -y \
    build-essential make cmake g++ ca-certificates \
    curl pkg-config python3.11 python3.11-dev python3.11-venv \
    python3.11-distutils git protobuf-compiler protobuf-compiler-grpc \
    sqlite3 libssl-dev libclang-dev librocksdb-dev
RUN apt -y remove python3.10

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y

ENV PATH="/root/.cargo/bin:${PATH}"

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
RUN bash -c 'export NVM_DIR="$([ -z "${XDG_CONFIG_HOME-}" ] && printf %s "${HOME}/.nvm" || printf %s "${XDG_CONFIG_HOME}/nvm")" && \. "$NVM_DIR/nvm.sh" && nvm install stable'

COPY ./ .

COPY .git .git

ENV PYTHON=python3.11
ENV PYO3_CROSS_PYTHON_VERSION=3.11
ENV PYTHONPATH=${PYTHONPATH}:/indexify-build/extractors
RUN bash -c 'export NVM_DIR="$([ -z "${XDG_CONFIG_HOME-}" ] && printf %s "${HOME}/.nvm" || printf %s "${XDG_CONFIG_HOME}/nvm")" && \. "$NVM_DIR/nvm.sh" && cargo build --release'
