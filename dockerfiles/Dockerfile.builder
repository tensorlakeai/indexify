FROM ubuntu:22.04
LABEL stage=builder

WORKDIR /indexify-build

COPY ./ .

COPY .git .git

RUN apt-get update

RUN apt-get install -y \
    build-essential \
    curl pkg-config python3.11 python3.11-dev python3.11-venv git

RUN apt -y install protobuf-compiler protobuf-compiler-grpc sqlite3 libssl-dev

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN cargo build --release
