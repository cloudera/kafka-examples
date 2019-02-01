_Copyright &copy; Cloudera, Inc. 2019_
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

  2.  Copy the target/streaming-ref-app-simple-0.1-SNAPSHOT-jar-with-dependencies.jar, db/init_kudu_db.sql and all files
  from the ../scripts directory to a host on the cluster.
  3.  Ssh to that host
  4.  Edit config.sh. It contains reasonable defaults, make sure that you set each value fitting to your environment.  
  5.  Execute all the other .sh files. They will generate various config files used by the application:
      1. `kudu.sh` - creates the streaming_ref datbase and the tables in Kudu and sentry privileges required to access them
      2. `producer.sh` - creates sentry privileges and configuration files for the application that generates
      input records and sends them to kafka
      3. `spark-kafka.sh` - creates Kafka related sentry privileges and configuration files for spark application
      4. `topics.sh` - creates the Kafka topics

### Using the application
The StructuredStreamingApp application will read records from the transactions Kafka topic and it will
write enriched data to the transactions Kudu table. It will also write the operational_metadata table.

When the application is submitted to the Spark service it will start the streaming pipeline,
but it will not produce any output until it gets data from the Kafka topic.

You can send data to the topic using a ConsoleProducer to producer or by implementing a custom producer.
This sample shows the required format:

`{"transaction_id": "1", "customer_id": 1,  "vendor_id": 1, "event_state": "CREATED", "event_timestamp": "2018-11-12 09:42:00", "price": "100", "card_type": "Credit"} `

You can check the output e.g. by using Impala.
* Authenticate with `kinit` using a user that has access to the streaming_ref database and all the tables you want to check
* `impala-shell -i <hostname> -k --ssl`
* in the shell execute
  ```
  use streaming_ref;

  select count(*) from transactions
  ```
  to quickly check if the application is producing output, or you can execute any other queries against the output tables.


#### Submitting the application without security

 Execute

  ```
  spark-submit --files consumer.properties \      
  --class com.cloudera.streaming.refapp.StructuredStreamingApp --deploy-mode cluster \  
  --master yarn streaming-ref-app-simple-0.1-SNAPSHOT-jar-with-dependencies.jar \  
  consumer.properties <kudu_master_host:kudu_master_port>
  ```  

#### Submitting the application on a secured cluster

  Execute

  ```
  kinit -kt <keytab> <SPARK_USER>

  spark-submit --files consumer.properties,kafka_client_jaas.conf,<keytab file> --driver-java-options \
  "-Djava.security.auth.login.config=./kafka_client_jaas.conf"  --class com.cloudera.streaming.refapp.StructuredStreamingApp \
  --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf" \
  --deploy-mode cluster --master yarn streaming-ref-app-simple-0.1-SNAPSHOT-jar-with-dependencies.jar \
  consumer.properties <kudu_master_host:kudu_master_port>
  ```

The application will keep running until you kill it in yarn.
