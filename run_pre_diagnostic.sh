#!/bin/bash
cd ~/dinamo-etl-kafka/src
PYSPARK_PYTHON=$(which python) \
PYTHONPATH=$(pwd):$PYTHONPATH \
spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  ./pre_diagnostic_engine/pipeline/pre_diagnostic_pipeline.py