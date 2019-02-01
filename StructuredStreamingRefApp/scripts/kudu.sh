#!/bin/bash
# Copyright (C) Cloudera, Inc. 2019

set -fex

# Creates the streaming_ref datbase and the tables in Kudu and sentry privileges required to access them

. $(dirname $0)/config.sh

SQL_FILE=$(dirname $0)/init_kudu_db.sql

if [ -z ${IMPALA_DAEMON} ]
then
  : "\${IMPALA_DAEMON} must be set to the hots[:port] value the shell can connect to"
  exit 1
fi

# Create sentry role for Spark if it does not exist
kinit -kt $SETUP_KEYTAB $SETUP_PRINCIPAL
if kafka-sentry -lr | grep -q $SPARK_ROLE ; then
    echo $SPARK_ROLE already exists
else
    : "Create sentry role $SPARK_ROLE"
    kafka-sentry -cr -r $SPARK_ROLE
fi

kinit -kt $DB_ADMIN_KEYTAB $DB_ADMIN_USER

impala-shell -i ${IMPALA_DAEMON} -f ${SQL_FILE} -k --ssl

impala-shell -i ${IMPALA_DAEMON} -k --ssl -q "GRANT ALL ON DATABASE streaming_ref to ${SPARK_ROLE}"

kdestroy
