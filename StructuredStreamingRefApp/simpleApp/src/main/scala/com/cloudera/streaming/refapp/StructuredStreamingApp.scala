/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.io.{FileInputStream, InputStream}
import java.sql.Timestamp
import java.util.{Properties, UUID}

import scala.collection.JavaConverters._

import org.apache.kudu.spark.kudu._

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession, functions}

/**
  * A long running streaming application that can be submitted to a running Spark service.
  * It reads static data from Kudu, streaming data from Kafka and writes output to Kudu.
  * It demonstrates
  * - joining the two datasets
  * - performing simple validation
  * - working with time windows to create aggregate statistics
  * It focuses on the kafka-spark-kudu integration and keeps all other aspects as simple as possible.
  */
object StructuredStreamingApp {

  case class Transaction(transaction_id: String,
                         customer_id: Option[Int],
                         vendor_id: Option[Int],
                         event_state: Option[String],
                         event_timestamp: Timestamp,
                         price: Option[String],
                         card_type: Option[String])

  def main(args: Array[String]) {

    if (args.length < 2) {
      sys.error(
        """Usage:
          |com.cloudera.streaming.refapp.StructuredStreamingApp consumer.config kudu-master
          |consumer.config path to kafka client properties
          |kudu-master host:port pair pointing to a kudu master instance
        """.stripMargin)
    }

    // extract first two arguments
    val Array(consumerConfig, kuduMaster) = args
    val kafkaParamsForSpark = kafkaConfigFromPropertiesFile(consumerConfig)

    val spark = SparkSession.builder().appName("streaming-ref").getOrCreate()

    import spark.implicits._

    def readKafkaStream(topic: String, schema: StructType) = {
      val kafkaOptions = kafkaParamsForSpark ++ Map("subscribe" -> topic, "startingoffsets" -> "latest")

      val df = spark.readStream.format("kafka").options(kafkaOptions).load()
        .selectExpr("CAST(value AS STRING)")
        .select(functions.from_json('value, schema) as "parsedValue")
        .selectExpr("parsedValue.*")
      df.createOrReplaceTempView(topic)
      df
    }

    val kuduDatabase = "streaming_ref"

    def readKuduTable(name: String) = {
      val fullTableName = s"impala::$kuduDatabase.$name"
      val df = spark
        .read
        .options(Map(
            "kudu.master" -> kuduMaster,
            "kudu.table" -> fullTableName))
        .kudu
      df.createOrReplaceTempView(name)
      df
    }

    // Checkpointing is needed for failure handling: streaming queries that use checkpointing
    // can be continued after a faileru where the failed one left off, ensuring data consistency guarantees.
    // Each query needs a unique checkpoint location, that's why a random UUID is used.
    // In production you may want to set it to a stable, but unique, reliable location (e.g. on HDFS).
    val baseCheckpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString

    def writeKuduTable(df: DataFrame, name: String) = {
      val fullTableName = s"impala::$kuduDatabase.$name"
      df
        .writeStream
        .format("kudu")
        .option("kudu.master", kuduMaster)
        .option("kudu.table", fullTableName)
        .option("checkpointLocation", s"$baseCheckpointLocation/$name")
        .option("retries", "3")
        .outputMode("update")
        .start()
    }

    val customers = readKuduTable("customers")

    val transactions = readKafkaStream("transaction", Encoders.product[Transaction].schema)
      .withWatermark("event_timestamp", "1 minute")

    val enrichedTransactions = spark.sql(
      """
        SELECT
          t.*,
          c.first_name as customer_first_name, c.last_name as customer_last_name,
          card_type in ('Visa', 'MasterCard') and event_state in ('created') as is_valid
        FROM transaction t
        LEFT OUTER JOIN customers c ON t.customer_id = c.customer_id
      """.stripMargin)

    writeKuduTable(enrichedTransactions, "transactions")

    val operationalMetadata = transactions
      .groupBy(functions.window(functions.col("event_timestamp"), "1 minutes"))
      .count().as("c")
      .selectExpr("c.window.start as start_ts", "c.window.end as end_ts", "c.count as num_transactions")

    writeKuduTable(operationalMetadata, "operational_metadata")

    spark.streams.awaitAnyTermination()

  }

  def kafkaConfigFromPropertiesFile(configFile: String) = {
    var inputStream: Option[InputStream] = None
    val kafkaParams = try {
      inputStream = Some(new FileInputStream(configFile))
      val params = new Properties()
      params.load(inputStream.get)
      inputStream.get.close()
      params.asScala.toMap
    } finally {
      inputStream.foreach(_.close())
    }
    kafkaParams.map {
      case (key, value) => "kafka." + key -> value
    }
  }

}