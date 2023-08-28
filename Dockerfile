FROM ubuntu:22.04 AS builder
LABEL stage=builder

WORKDIR /indexify-build

COPY ./ .

RUN apt-get update

RUN apt-get install -y \
    build-essential \
    curl pkg-config python3 python3-dev

RUN apt -y install protobuf-compiler protobuf-compiler-grpc sqlite3 libssl-dev

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN cargo build --release

RUN cargo build --package migration --release

FROM ubuntu:22.04

RUN apt update

RUN apt install -y libssl-dev gcc python3-venv python3-dev

RUN python3 -m "venv" /venv && /venv/bin/pip install torch --index-url https://download.pytorch.org/whl/cpu

RUN /venv/bin/pip install transformers[torch] optimum[onnxruntime] onnx onnxruntime pydantic span_marker

WORKDIR /indexify

COPY --from=builder /indexify-build/target/release/indexify ./

COPY --from=builder /indexify-build/target/release/migration ./

COPY --from=builder /indexify-build/sample_config.yaml ./config/indexify.yaml

COPY --from=builder /indexify-build/indexify_extractors/ /indexify/indexify_extractors/

COPY --from=builder /indexify-build/setup.py /indexify/

COPY ./scripts/docker_compose_start.sh .

RUN /venv/bin/pip install .

ENV PATH=/venv/bin:$PATH

# This serves as a test to ensure the binary actually works
CMD [ "/indexify/indexify", "start", "-c", "./config/indexify.yaml" ]
