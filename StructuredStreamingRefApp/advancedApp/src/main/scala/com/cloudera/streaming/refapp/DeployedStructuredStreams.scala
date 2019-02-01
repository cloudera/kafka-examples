/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * A long running streaming application that can be submitted to a running Spark service.
  * It reads static data from Kudu, streaming data from Kafka and writes output to Kudu
  */
object DeployedStructuredStreams {

  def main(args: Array[String]) {

    val logger = LoggerFactory.getLogger(getClass)

    if (args.length < 2) {
      sys.error(
        """Usage:
          |com.cloudera.streaming.refapp.DeployedStructuredStreams consumer.config kudu-master timeToLive
          |consumer.config path to kafka client properties
          |kudu-master host:port pair pointing to a kudu master instance
          |timeToLive optional, if specified the application will be stopped after timeToLive seconds, useful for testing
        """.stripMargin)
    }
    // extract first two arguments
    val Array(consumerConfig, kuduMaster, _*) = args
    // read optional argument
    val timeToLive = if (args.length > 2) Some(args(2).toInt) else None

    val spark = SparkSession.builder().appName("streaming-ref").getOrCreate()

    val kafkaConfig: KafkaConfig = KafkaConfig.fromPropertiesFile(consumerConfig)
    val kafkaSource = new KafkaSource(spark, kafkaConfig)

    val kuduDatabase = "streaming_ref"
    val kuduSource = new KuduSource(spark, kuduMaster, kuduDatabase)
    val kuduSink = new KuduSink(kuduMaster, kuduDatabase, defaultCheckpointLocation)

    val application = new Application(
      spark,
      Sources(
        statesFromCluster = kuduSource.loadTable("states"),
        customersFromCluster = kuduSource.loadTable("customers"),
        vendorsFromCluster = kuduSource.loadTable("vendors"),
        customersFromStream = kafkaSource.jsonStreamWithKafkaTimestamp("customer"),
        vendorsFromStream = kafkaSource.jsonStreamWithTimestampFromMessage("vendor", "update_timestamp"),
        transactionsFromStream = kafkaSource.jsonStreamWithTimestampFromMessage("transaction", "event_timestamp")
      ),
      Sinks(
        validTransactions = kuduSink.writeTable("valid_transactions"),
        invalidTransactions = kuduSink.writeTable("invalid_transactions"),
        customerOrphans = kuduSink.writeTable("customer_orphans"),
        vendorOrphans = kuduSink.writeTable("vendor_orphans"),
        customers = kuduSink.writeTable("customers"),
        vendors = kuduSink.writeTable("vendors"),
        transactionsOperationalMetadata = kuduSink.writeTable("transactions_operational_metadata")
      ))

    application.start()

    timeToLive match {
      case Some(tl) =>
        logger.info(s"Running application for $tl seconds")
        Thread.sleep(tl * 1000)
        logger.info("Stopping application")
      case None => spark.streams.awaitAnyTermination()
    }
  }
}
