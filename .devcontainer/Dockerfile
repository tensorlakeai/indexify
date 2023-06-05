FROM rust:latest AS builder

RUN update-ca-certificates

RUN apt update

RUN apt -y install protobuf-compiler protobuf-compiler-grpc sqlite3 python3-dev pip virtualenv python3
