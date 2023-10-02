FROM ubuntu
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update

RUN apt -y install software-properties-common 
RUN add-apt-repository ppa:deadsnakes/ppa -y

RUN apt update

RUN apt -y install python3.11 python3.11-dev python3.11-venv sqlite3 pip virtualenv
RUN apt -y install protobuf-compiler protobuf-compiler-grpc
RUN apt -y install ca-certificates curl gnupg pkg-config libssl-dev vim

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y
