#!/bin/bash

set -ex

function stop_docker()
{
  echo "stopping qdrant_test"
  docker stop qdrant_test
  docker stop postgres_test
}

# Ensure current path is project root
#cd "$(dirname "$0")/../"

QDRANT_VERSION='v1.4.1'
QDRANT_HOST='localhost:6333'

docker run -d --rm \
           --network=host \
           --name qdrant_test qdrant/qdrant:${QDRANT_VERSION}

docker run --rm -p 5432:5432 \
          --name=postgres_test -e POSTGRES_PASSWORD=postgres \
          -e POSTGRES_DB=indexify_test -d ankane/pgvector

trap stop_docker SIGINT
trap stop_docker ERR

until curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/collections; do
  printf 'waiting for server to start...'
  sleep 5
done

timeout 90s bash -c "until docker exec postgres_test pg_isready ; do sleep 5 ; done"
docker exec postgres_test psql -U postgres -c 'create database indexify;'

pip install .

cargo test -- --test-threads 1

echo "Ok, that is enough"

stop_docker
