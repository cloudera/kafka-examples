#!/bin/bash
# Copyright (C) Cloudera, Inc. 2019

set -fex

# Creates Kafka topics

. $(dirname $0)/config.sh

for TOPIC in $TOPICS
do
  : "Create topic $TOPIC"
  kafka-topics --create --zookeeper $ZOOKEEPER_QUORUM \
    --replication-factor 3 \
    --partitions 24 --topic $TOPIC
done
