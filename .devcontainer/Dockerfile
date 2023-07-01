FROM ubuntu

RUN apt update

RUN apt -y install protobuf-compiler protobuf-compiler-grpc sqlite3 python3-dev pip virtualenv python3
RUN apt -y install ca-certificates curl gnupg pkg-config libssl-dev
#RUN install -m 0755 -d /etc/apt/keyrings
#RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
#RUN chmod a+r /etc/apt/keyrings/docker.gpg


#RUN echo \
#  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
#  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
#  tee /etc/apt/sources.list.d/docker.list > /dev/null

#RUN apt update
#RUN apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y
