FROM rust:latest AS builder

RUN update-ca-certificates

WORKDIR /indexify-build

COPY ./ .

RUN ./setup_libtorch_cpu.sh

RUN apt update

RUN apt -y install protobuf-compiler protobuf-compiler-grpc sqlite3 python3-dev

RUN cargo install sea-orm-cli
