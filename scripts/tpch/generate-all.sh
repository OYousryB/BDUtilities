#!/usr/bin/env bash
source scripts/env.txt

sbt clean package assembly

echo "Generating Data"
$DB_UTILITIES/scripts/tpch/generate-data.sh

echo "Generating parquet"
$DB_UTILITIES/scripts/tpch/generate-parquet.sh
