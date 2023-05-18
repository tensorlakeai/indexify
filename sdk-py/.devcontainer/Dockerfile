ARG VARIANT="3"

FROM mcr.microsoft.com/vscode/devcontainers/python:${VARIANT}

ARG POETRY_VERSION="1.3.1"
ARG POETRY_SRC="https://install.python-poetry.org"

USER vscode
WORKDIR /home/vscode

RUN curl -fsSL -o install-poetry.py "${POETRY_SRC}" \
 && python install-poetry.py --version $POETRY_VERSION \
 && rm install-poetry.py

RUN mkdir -p .config/git \
 && echo ".vscode/*" >> .config/git/ignore \
 && echo "*.code-workspace" >> .config/git/ignore \
 && echo ".history/" >> .config/git/ignore
