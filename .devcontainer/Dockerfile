FROM ubuntu

RUN apt update

RUN apt -y install protobuf-compiler protobuf-compiler-grpc sqlite3 python3-dev pip virtualenv python3
RUN apt -y install ca-certificates curl gnupg pkg-config libssl-dev


RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y