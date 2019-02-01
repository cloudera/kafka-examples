#!/bin/bash
# Copyright (C) Cloudera, Inc. 2019

set -fex

# Creates sentry privileges and configuration files for producer application

. $(dirname $0)/config.sh

kinit -kt $SETUP_KEYTAB $SETUP_PRINCIPAL
: "Create sentry role $PRODUCER_ROLE"
kafka-sentry -cr -r $PRODUCER_ROLE


: "Add role $PRODUCER_ROLE to group $PRODUCER_GROUP"
kafka-sentry -arg -r $PRODUCER_ROLE -g $PRODUCER_GROUP

for TOPIC in $TOPICS
do
  for PRODUCER_HOST in $PRODUCER_HOSTS
  do
    : "Grant privileges to role $PRODUCER_ROLE on topic $TOPIC from host $PRODUCER_HOST"
    kafka-sentry -gpr -r $PRODUCER_ROLE -p "Host=$PRODUCER_HOST->Topic=$TOPIC->action=describe"
    kafka-sentry -gpr -r $PRODUCER_ROLE -p "Host=$PRODUCER_HOST->Topic=$TOPIC->action=write"
  done
done

: "Create client.properties"
cat >$PRODUCER_FILES_DIR/producer.properties<<EOF
bootstrap.servers=${BROKER_LIST}
security.protocol=SASL_SSL
sasl.kerberos.service.name=kafka
ssl.truststore.location=${PRODUCER_TRUSTSTORE_LOCATION}
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
  useKeyTab=true \
  storeKey=true  \
  useTicketCache=true \
  keyTab="${PRODUCER_KEYTAB}" \
  principal="${PRODUCER_USER}@${PRODUCER_REALM}";
EOF

kdestroy
