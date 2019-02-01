# Copyright (C) Cloudera, Inc. 2019

# Configuration for setup scripts
# Should be sourced by them

: "Kerberos principal used for setup: ${SETUP_PRINCIPAL:=kafka}"

: "Keytab used to login as setup principal: ${SETUP_KEYTAB:=/cdep/keytabs/$SETUP_PRINCIPAL.keytab}"

: "Input topics to set up: ${TOPICS:=customer vendor transaction}"

: "Zookeeper quorum: ${ZOOKEEPER_QUORUM:=$(hostname):2181}"

: "Kafka broker list: ${BROKER_LIST:=$(hostname):9093}"

# Producer
: "Hosts producer connects from: ${PRODUCER_HOSTS:=*}"

: "Producer user/principal: ${PRODUCER_USER:=flume}"

: "Keytab used to login as producer: ${PRODUCER_KEYTAB:=/cdep/keytabs/$PRODUCER_USER.keytab}"

: "Kerberos realm: ${PRODUCER_REALM:=`klist -kt $PRODUCER_KEYTAB | grep '@' | head -n 1 | sed s/.*@// | sed s/[[:space:]]//`}"

: "Primary group of producer user: ${PRODUCER_GROUP:=`id -gn $PRODUCER_USER`}"

: "Producer Sentry role: ${PRODUCER_ROLE:=$PRODUCER_GROUP}"

: "Producer truststore location: ${PRODUCER_TRUSTSTORE_LOCATION:=/etc/cdep-ssl-conf/CA_STANDARD/truststore.jks}"

: "Directory to store producer's files: ${PRODUCER_FILES_DIR:=`pwd`}"


# Spark application as Kafka consumer
: "Hosts Spark application consumes from: ${SPARK_HOSTS:=*}"

: "Producer user/principal: ${SPARK_USER:=systest}"

: "Keytab used by Spark application: ${SPARK_KEYTAB:=/cdep/keytabs/$SPARK_USER.keytab}"

: "Kerberos realm: ${SPARK_REALM:=`klist -kt $SPARK_KEYTAB | grep '@' | head -n 1 | sed s/.*@// | sed s/[[:space:]]//`}"

: "Primary group of consumer user: ${SPARK_GROUP:=`id -gn $SPARK_USER`}"

: "Consumer Sentry role: ${SPARK_ROLE:=$SPARK_GROUP}"

: "Consumer truststore location: ${SPARK_TRUSTSTORE_LOCATION:=/etc/cdep-ssl-conf/CA_STANDARD/truststore.jks}"

: "Directory to store consumer's files: ${CONSUMER_FILES_DIR:=`pwd`}"

# Database
: "Database admin user: ${DB_ADMIN_USER:=impala}"

: "Keytab used to login as db admin user: ${DB_ADMIN_KEYTAB:=/cdep/keytabs/$DB_ADMIN_USER.keytab}"

: "Impala daemon to connect: ${IMPALA_DAEMON}"
