FROM ubuntu:20.04
#LABEL stage=builder

WORKDIR /indexify-build
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt -y install software-properties-common unzip \
    build-essential make cmake ca-certificates \
    curl pkg-config git \
    sqlite3 clang gcc-10 g++-10

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y

RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v25.1/protoc-25.1-linux-x86_64.zip

RUN unzip protoc-25.1-linux-x86_64.zip -d /usr/local

ENV PATH="/${HOME}/.cargo/bin:/{HOME}/.local/bin:${PATH}"

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN curl -sL https://deb.nodesource.com/setup_22.x | bash

RUN apt install -y nodejs

RUN apt remove -y gcc-9 g++-9
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 20 --slave /usr/bin/g++ g++ /usr/bin/g++-10
