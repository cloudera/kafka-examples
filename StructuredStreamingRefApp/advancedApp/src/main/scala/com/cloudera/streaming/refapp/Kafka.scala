/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
  * Reads/builds configuration for kafka clients that are either standalone or live in Spark.
  */
object KafkaConfig {

  /**
    * Reads configuration from property file
    */
  def fromPropertiesFile(configFile : String) = {
    var inputStream : Option[InputStream] = None
    val kafkaParams = try {
      inputStream = Some(new FileInputStream(configFile))
      val params = new Properties()
      params.load(inputStream.get)
      inputStream.get.close()
      params.asScala.toMap
    } finally {
      inputStream.foreach(_.close())
    }
    new KafkaConfig(kafkaParams)
  }

  /**
    * Builds configuration for brokers using PLAINTEXT protocol.
    */
  def fromBrokerList(bootstrapServers: String) = {

    new KafkaConfig(Map[String, String](
      "bootstrap.servers" -> bootstrapServers,
      "security.protocol" -> "PLAINTEXT"))
  }
}

class KafkaConfig(val kafkaParams : Map[String, String]) {

  /**
    * Converts plain kafka configuration for usage in Spark.
    */
  val kafkaParamsForSpark: Map[String, String] = kafkaParams.map {
    case (key, value) => "kafka." + key -> value
  }
}
/**
  * Creates streaming Sources reading Kafka topics.
  */
class KafkaSource(spark: SparkSession, kafkaConfig: KafkaConfig) {

  private def loadStream(topic:String, startingOffset: String) = {

    val params = kafkaConfig.kafkaParamsForSpark +
      ("subscribe" -> topic) +
      ("startingoffsets" -> startingOffset)
    spark.readStream.format("kafka").options(params).load()
  }

  /**
    * Creates a dataframe from a kafka topic containing Strings.
    * Useful for testing and debugging.
    */
  def stringStream(topic: String): DataFrame = loadStream(topic, "earliest").selectExpr("CAST(value AS STRING)")

  /**
    * Creates a streaming source that reads JSON records.
    * The DataFrame will include the timestamp that kafka added to the message, called Schemas.timestampColumnName.
    */
  def jsonStreamWithKafkaTimestamp(topic: String)(schema: StructType): DataFrame = {

    import spark.implicits._

    loadStream(topic, "latest")
      .withColumn(Schemas.timestampColumnName, functions.col("timestamp"))
      .selectExpr("CAST(value AS STRING)", Schemas.timestampColumnName)
      .select(functions.from_json('value, schema) as "entity", functions.col(Schemas.timestampColumnName))
      .select("entity.*", Schemas.timestampColumnName)
  }

  /**
    * Creates a streaming source that reads JSON records.
    * The DataFrame will include the timestamp from the original message, called Schemas.timestampColumnName.
    */
  def jsonStreamWithTimestampFromMessage(topic: String, timestampColumnName: String)(schema: StructType): DataFrame = {

    import spark.implicits._

    // TODO simplify selects
    loadStream(topic, "latest")
      .selectExpr("CAST(value AS STRING)")
      .select(functions.from_json('value, schema) as "entity")
      .selectExpr("entity.*", s"entity.$timestampColumnName as ${Schemas.timestampColumnName}")
  }
}