#!/bin/bash

# Setup schema
export DATABASE_URL=postgres://postgres:postgres@172.20.0.5/indexify
until /indexify/migration up
do
  echo "Trying to apply schema again in 5 seconds...."
  sleep 5s
done

# Srart server
/indexify/indexify start -c ./config/indexify.yaml
