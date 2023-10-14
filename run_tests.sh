#!/bin/bash

set -ex

function stop_docker()
{
  echo "stopping qdrant_test"
  docker stop indexify-local-qdrant
  docker stop indexify-local-postgres
  docker stop indexify-local-opensearch
}

QDRANT_HOST='localhost:6333'

trap stop_docker SIGINT
trap stop_docker ERR

make local-dev

until curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/collections; do
  printf 'waiting for server to start...'
  sleep 5
done

pip install .

cargo test -- --test-threads 1

echo "Ok, that is enough"

stop_docker
