#!/usr/bin/env bash

set -e

pushd pyspark-neo4j
  make dist
popd
