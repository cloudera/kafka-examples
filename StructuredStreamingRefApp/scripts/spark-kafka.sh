#!/bin/bash
# Copyright (C) Cloudera, Inc. 2019

set -fex

# Creates Kafka related sentry privileges and configuration files for Spark application

. $(dirname $0)/config.sh

# Create sentry role for Spark if it does not exist
kinit -kt $SETUP_KEYTAB $SETUP_PRINCIPAL
if kafka-sentry -lr | grep -q $SPARK_ROLE ; then
    echo $SPARK_ROLE already exists
else
    : "Create sentry role $SPARK_ROLE"
    kafka-sentry -cr -r $SPARK_ROLE
fi

: "Add role $SPARK_ROLE to group $SPARK_GROUP"
kafka-sentry -arg -r $SPARK_ROLE -g $SPARK_GROUP

for SPARK_HOST in $SPARK_HOSTS
do
  for TOPIC in $TOPICS
  do
    : "Grant privileges to role $SPARK_ROLE on topic $TOPIC from host $SPARK_HOST"
    kafka-sentry -gpr -r $SPARK_ROLE -p "Host=$SPARK_HOST->Topic=$TOPIC->action=describe"
    kafka-sentry -gpr -r $SPARK_ROLE -p "Host=$SPARK_HOST->Topic=$TOPIC->action=read"
    : "Allow role $SPARK_ROLE to join any consumer group from host $SPARK_HOST"
    kafka-sentry -gpr -r $SPARK_ROLE -p "Host=$SPARK_HOST->Consumergroup=*->action=describe"
    kafka-sentry -gpr -r $SPARK_ROLE -p "Host=$SPARK_HOST->Consumergroup=*->action=read"
  done
done

: "Create jaas.config"
cat > kafka_client_jaas.conf<<EOF
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="$SPARK_KEYTAB"
    useTicketCache=false
    serviceName="kafka"
    principal="$SPARK_USER@${SPARK_REALM}";
};
EOF


: "Create client.properties"
cat >$CONSUMER_FILES_DIR/consumer.properties<<EOF
bootstrap.servers=${BROKER_LIST}
security.protocol=SASL_SSL
sasl.kerberos.service.name=kafka
ssl.truststore.location=${SPARK_TRUSTSTORE_LOCATION}
EOF


kdestroy
