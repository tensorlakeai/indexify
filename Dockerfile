FROM rust:latest AS builder
LABEL stage=builder

RUN update-ca-certificates

WORKDIR /indexify-build

COPY ./ .

RUN ./setup_libtorch_cpu.sh

ENV LIBTORCH=/indexify-build/libtorch

ENV LD_LIBRARY_PATH=${LIBTORCH}/lib:$LD_LIBRARY_PATH

RUN apt update 

RUN apt -y install protobuf-compiler protobuf-compiler-grpc sqlite3

RUN sqlite3 new_indexify.db "VACUUM;"

RUN cargo build --release

RUN cargo install sea-orm-cli

RUN DATABASE_URL=sqlite://new_indexify.db sea-orm-cli migrate up

RUN ["./target/release/indexify", "init-config", "indexify.yaml"]


# FROM ubuntu:20.04
# apt update && apt install -y libgomp1 libssl-dev
FROM gcr.io/distroless/cc

WORKDIR /indexify

COPY --from=builder /indexify-build/target/release/indexify ./

COPY --from=builder /indexify-build/libtorch ./libtorch

COPY --from=builder /indexify-build/sample_config.yaml ./config/indexify.yaml

COPY --from=builder /indexify-build/new_indexify.db ./indexify.db

ENV LIBTORCH=/indexify/libtorch

ENV LD_LIBRARY_PATH=${LIBTORCH}/lib:$LD_LIBRARY_PATH

ENTRYPOINT [ "/indexify/indexify" ]

CMD [ "start", "-c", "./config/indexify.yaml" ]