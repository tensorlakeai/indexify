# Indexify Executor 

[![PyPI version](https://badge.fury.io/py/indexify-extractor-sdk.svg)](https://badge.fury.io/py/indexify-extractor-sdk)

Indexify Executor is the data plane for Indexify server. It executes serialized Graphs and sends the function outputs 
back to the server

## Install

```bash
virtualenv ve
source ve/bin/activate
pip install indexify-executor
```

## Start

```bash
indexify executor start --server-addr localhost:8900
```
