/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.sql.Timestamp

import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.Encoders

class LocalIntegrationTest extends IntegrationTestBase {

  test("Integration test with one kafka and one spark instance embedded in the same JVM") {

    val inputDir = "src/test/resources/samples"

    val spark = EmbeddedSpark.sparkSession

    val fileSource = new FileSources(spark, inputDir)
    val kafkaConfig = EmbeddedKafkaBroker.defaultKafkaConfig
    val kafkaSource = new KafkaSource(spark, kafkaConfig)

    val application = new Application(
      spark,
      Sources(
        statesFromCluster = fileSource.jsonFile("states"),
        customersFromCluster = fileSource.jsonFile("customers"),
        vendorsFromCluster = fileSource.jsonFile("vendors"),
        customersFromStream = kafkaSource.jsonStreamWithKafkaTimestamp("customer"),
        vendorsFromStream = kafkaSource.jsonStreamWithTimestampFromMessage("vendor", "update_timestamp"),
        transactionsFromStream = kafkaSource.jsonStreamWithTimestampFromMessage("transaction", "event_timestamp")
      ),
      Sinks(
        invalidTransactions = Memory.memorySink("invalidTransactions"),
        validTransactions = Memory.memorySink("validTransactions"),
        customerOrphans = Memory.memorySink("customerOrphans"),
        vendorOrphans = Memory.memorySink("vendorOrphans"),
        customers = Memory.memorySink("customers"),
        vendors = Memory.memorySink("vendors"),
        transactionsOperationalMetadata = Memory.memorySink("transactionsOperationalMetadata")
      ))

    application.start()

    eventually(timeout(Span(20, Seconds)), interval(Span(5, Seconds))) {
      EmbeddedKafkaBroker.publishStringMessageToKafka(
        "transaction",
        """{
          "transaction_id": "1",
          "customer_id": 1,
          "vendor_id": 1,
          "event_state": "CREATED",
          "event_timestamp": "2018-11-12 09:42:00",
          "price": "100",
          "card_type": "Credit"}""")
      EmbeddedKafkaBroker.publishStringMessageToKafka(
        "transaction",
        """{
          "transaction_id": "21",
          "customer_id": 100,
          "vendor_id": 2,
          "event_state": "SWIPED",
          "event_timestamp": "2018-11-13 09:45:01",
          "price": "100",
          "card_type": "Debit"}""")

      val validTransactionsQuery = application.streamingQueries.validTransactions
      validTransactionsQuery.processAllAvailable()
      val currentContent = spark.table("validTransactions").as[Transaction](Encoders.product).collect()

      currentContent.shouldBe(
        Array(
          Transaction(
            transaction_id = "1",
            customer_id = Some(1),
            vendor_id = Some(1),
            event_state = Some("CREATED"),
            event_timestamp = Timestamp.valueOf("2018-11-12 09:42:00"),
            price = Some("100"),
            card_type = Some("Credit")),
          Transaction(
            transaction_id = "21",
            customer_id = Some(100),
            vendor_id = Some(2),
            event_state = Some("SWIPED"),
            event_timestamp = Timestamp.valueOf("2018-11-13 09:45:01"),
            price = Some("100"),
            card_type = Some("Debit"))
        ))
    }
  }
}