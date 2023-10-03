# Credits to https://gist.github.com/runeb/ed1a64859a7c762d9b6f5d41a8c63521
FROM postgres:15.3

RUN apt update && apt upgrade -y
RUN apt install -y git build-essential postgresql-server-dev-15

# Copy in files from ph_embedding dir
RUN git clone https://github.com/neondatabase/pg_embedding.git
RUN cd pg_embedding && make && make install
