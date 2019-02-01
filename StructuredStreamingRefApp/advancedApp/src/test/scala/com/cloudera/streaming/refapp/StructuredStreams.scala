/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.time.Duration

object StructuredStreams {

  def main(args: Array[String]) {

    if (args.length != 3) {
      sys.error(
        """Usage:
          |com.cloudera.streaming.refapp.StructuredStreams inputDir outputDir kudu-master
          |inputDir should have the same structure as the src/main/resources/samples directory of this project
          |outputDir is created if it does not exist and it's purged if it exists
          |kudu-master host:port pair pointing to a kudu master instance""".stripMargin)
    }
    val Array(inputDir, outputDir, kuduMaster) = args

    val spark = EmbeddedSpark.sparkSession

    val fileSource = new FileSources(spark, inputDir)
    val fileSink = new FileSinks(outputDir, defaultCheckpointLocation)
    val kafkaConfig = EmbeddedKafkaBroker.defaultKafkaConfig
    val kafkaSource = new KafkaSource(spark, kafkaConfig)

    val kuduDatabase = "streaming_ref"
    val kuduSource = new KuduSource(spark, kuduMaster, kuduDatabase)
    val kuduSink = new KuduSink(kuduMaster, kuduDatabase, defaultCheckpointLocation)

    val application = new Application(
      spark,
      Sources(
        statesFromCluster = kuduSource.loadTable("states"),
        customersFromCluster = fileSource.jsonFile("customers"),
        vendorsFromCluster = kuduSource.loadTable("vendors"),
//        customersFromStream = fileSource.jsonStream("customers", "update_timestamp"),
//        vendorsFromStream = fileSource.jsonStream("vendors", "update_timestamp"),
//        transactionsFromStream = fileSource.jsonStream("transactions", "event_timestamp")
        customersFromStream = kafkaSource.jsonStreamWithKafkaTimestamp("customer"),
        vendorsFromStream = kafkaSource.jsonStreamWithTimestampFromMessage("vendor", "update_timestamp"),
        transactionsFromStream = kafkaSource.jsonStreamWithTimestampFromMessage("transaction", "event_timestamp")
      ),
      Sinks(
//        invalidTransactions = fileSink.csv("invalidTransactions"),
//        validTransactions = fileSink.csv("validTransactions"),
//        customerOrphans = fileSink.csv("customerOrphans"),
//        vendorOrphans = fileSink.csv("vendorOrphans"),
//        customers = fileSink.csv("customers"),
//        vendors = fileSink.csv("vendors"),
//        transactionsOperationalMetadata = fileSink.csv("transactionsOperationalMetadata")
        validTransactions = kuduSink.writeTable("valid_transactions"),
        invalidTransactions = kuduSink.writeTable("invalid_transactions"),
        customerOrphans = kuduSink.writeTable("customer_orphans"),
        vendorOrphans = kuduSink.writeTable("vendor_orphans"),
        customers = kuduSink.writeTable("customers"),
        vendors = kuduSink.writeTable("vendors"),
        transactionsOperationalMetadata = kuduSink.writeTable("transactions_operational_metadata")
      ),
      clusterStartup = EmbeddedKafkaBroker.start(),
      initSources = {
        CustomerGenerator(kafkaConfig, "customer").start()
        VendorGenerator(kafkaConfig, "vendor").start()
        TransactionGenerator(kafkaConfig, "transaction").start()
      },
      cleanOutput = fileSink.cleanOutputs,
      queryRestartDurations = Map("valid_transactions" -> Duration.ofMinutes(1))
    )

    application.start()
    spark.streams.awaitAnyTermination()
  }
}