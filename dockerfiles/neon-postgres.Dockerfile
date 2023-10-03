# Credits to https://gist.github.com/runeb/ed1a64859a7c762d9b6f5d41a8c63521
FROM postgres:15.3

RUN apt update && apt upgrade -y
RUN apt install -y git build-essential postgresql-server-dev-15

RUN cd /tmp && git clone --branch v0.5.0 https://github.com/pgvector/pgvector.git && cd pgvector && make && make install
