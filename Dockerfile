FROM diptanu/indexify-build AS builder
LABEL stage=builder

WORKDIR /indexify-build/indexify

COPY ./ .

ENV LIBTORCH=/indexify-build/libtorch

ENV LD_LIBRARY_PATH=${LIBTORCH}/lib:$LD_LIBRARY_PATH

RUN sqlite3 new_indexify.db "VACUUM;"

RUN cargo build --release

RUN DATABASE_URL=sqlite://new_indexify.db sea-orm-cli migrate up

RUN ["./target/release/indexify", "init-config", "indexify.yaml"]


# FROM ubuntu:20.04
# apt update && apt install -y libgomp1 libssl-dev
FROM gcr.io/distroless/cc

WORKDIR /indexify

COPY --from=builder /indexify-build/indexify/target/release/indexify ./

COPY --from=builder /indexify-build/libtorch ./libtorch

COPY --from=builder /indexify-build/indexify/sample_config.yaml ./config/indexify.yaml

COPY --from=builder /indexify-build/indexify/new_indexify.db ./indexify.db

ENV LIBTORCH=/indexify/libtorch

ENV LD_LIBRARY_PATH=${LIBTORCH}/lib:$LD_LIBRARY_PATH

ENTRYPOINT [ "/indexify/indexify" ]

CMD [ "start", "-c", "./config/indexify.yaml" ]
