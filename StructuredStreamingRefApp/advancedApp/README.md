_Copyright &copy; Cloudera, Inc. 2019_
# Executing the application
## Locally

   com.cloudera.streaming.refapp.StructuredStreams inputDir outputDir kudu-master

   It will start an embedded Kafka and Spark instance.
   It's intended to be used during development and testing.

   This version can read local json files or generated input for streams and local files
   or Kudu tables for the static datasets.
   It prodcues CSV output or writes to Kudu. You can easily change any sink or source in the code of StructuredStreams.

   For input you can use samples in the src/test/resources/samples directory of this project,
   or your own files organized to the same structure.

   As an alternative you can use a data generator that will keep producing randomized vendor, customer and
   transaction records until it is stopped.
   To use the data generator add / uncomment the following section to the constructor call of the
   Application in the StructuredStreams object:
   ```
   initSources = {
              CustomerGenerator(kafkaConfig, "customer").start()
              VendorGenerator(kafkaConfig, "vendor").start()
              TransactionGenerator(kafkaConfig, "transaction").start()
            },
   ```


## Submitting to Spark service running on a cluster

### Cluster requriements
The application can be deployed on a cluster that already has all the required services:
* Kafka
* Spark
* Kudu

their dependencies:
* Zookeeper
* HDFS

#### Secured cluster
* Kerberos (the application is tested with MIT Kerberos, AD should also be ok)
* Sentry

All the other services should be configured to use SSL/TLS, Kerberos for authentication and Sentry for authorization

The application itself does not require Impala but it is used by the init scripts to create the Kudu tables and insert sample initial data.

### Preparations

  1. Execute

  `mvn clean package`

  2.  Copy the target/streaming-ref-app-advanced-0.1-SNAPSHOT-jar-with-dependencies.jar, db/init_kudu_db.sql and all files
  from the ../scripts directory to a host on the cluster.
  3.  Ssh to that host
  4.  Edit config.sh. It contains reasonable defaults, make sure that you set each value fitting to your environment.  
  5.  Execute all the other .sh files. They will generate various config files used by the application:
      1. `kudu.sh` - creates the streaming_ref datbase and the tables in Kudu and sentry privileges required to access them
      2. `producer.sh` - creates sentry privileges and configuration files for the application that generates
      input records and sends them to Kafka
      3. `spark-kafka.sh` - creates Kafka related sentry privileges and configuration files for Spark application
      4. `topics.sh` - creates the Kafka topics


### Using the application
The DeployedStructuredStreams application will read records from 3 Kafka topics (customer, vendor, and transaction) and it will
write customer, vendor and transaction data to Kudu tables (customers, vendors, states, valid_transactions, invalid_transactions,
customer_orphans, vendor_orphans, transactions_operational_metadata).

When the application is started / submitted to the Spark service it will start the streaming pipeline,
but it will not produce any output until it gets data from the Kafka topic.

You can send data on your own (e.g. using a ConsoleProducer to producer records with the same JSON format as the sample files in
src/test/resources/samples/kafka) or you can use the DeployedDataGenerator application.

You can check the output e.g. by using Impala.
* Authenticate with `kinit` using a user that has access to the streaming_ref database and all the tables you want to check
* `impala-shell -i <hostname> -k --ssl`
* in the shell execute
  ```
  use streaming_ref;

  select 'valid_transactions' as table_name, count(*) from valid_transactions
  union
  select 'invalid_transactions', count(*) from invalid_transactions
  union
  select 'customer_orphans', count(*) from customer_orphans
  union
  select 'vendor_orphans', count(*) from vendor_orphans
  union
  select 'transactions_operational_metadata', count(*) from transactions_operational_metadata;
  ```
  to quickly check if the application is producing output, or you can execute any other queries against the output tables.


#### Submitting the application without security

 Execute

  ```
  spark-submit --files consumer.properties \  
  --class com.cloudera.streaming.refapp.DeployedStructuredStreams --deploy-mode cluster \
  --master yarn streaming-ref-app-advanced-0.1-SNAPSHOT-jar-with-dependencies.jar \
  consumer.properties <kudu_master_host:kudu_master_port>
  ```  

#### Submitting the application on a secured cluster

  Execute

  ```
  kinit -kt <keytab> <SPARK_USER>

  spark-submit --files consumer.properties,kafka_client_jaas.conf,<keytab file> --driver-java-options \
  "-Djava.security.auth.login.config=./kafka_client_jaas.conf"  --class com.cloudera.streaming.refapp.DeployedStructuredStreams \
  --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf" \
  --deploy-mode cluster --master yarn streaming-ref-app-advanced-0.1-SNAPSHOT-jar-with-dependencies.jar \
  consumer.properties <kudu_master_host:kudu_master_port>
  ```

By default the application will keep running until you kill it in yarn. You can use an additional timeToLive parameter when the
application is submitted, in this case the application will stop after the given time (in seconds). E.g.
```
spark-submit...streaming-ref-app-advanced-0.1-SNAPSHOT-jar-with-dependencies.jar consumer.properties <kudu_master_host:kudu_master_port> 600
```
will stop after 10 minutes.

### Running the data generator
Start `java -cp streaming-ref-app-advanced-0.1-SNAPSHOT-jar-with-dependencies.jar \
com.cloudera.streaming.refapp.DeployedDataGenerator producer.properties`.     

# Testing
TransactionsFlowUnitTest and LocalIntegrationTest demonstrate how to write unit tests and integration tests
for Spark Structured Streaming applications.
