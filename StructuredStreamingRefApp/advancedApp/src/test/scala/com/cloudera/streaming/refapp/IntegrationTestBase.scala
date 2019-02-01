/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.util.UUID

import org.scalatest.AppendedClues._
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory


abstract class IntegrationTestBase extends FunSuite with BeforeAndAfter {

  private val logger = LoggerFactory.getLogger(getClass)

  private var diagnosticQuery: Option[StreamingQuery] = None

  before {
    waitForOneMessage()
  }

  after {
    diagnosticQuery.foreach(query => query.stop())
  }

  private def waitForOneMessage() = {
    EmbeddedKafkaBroker.start()

    val spark = EmbeddedSpark.sparkSession

    val topicName = UUID.randomUUID().toString.replaceAll("-", "_")

    val source = new KafkaSource(spark, EmbeddedKafkaBroker.defaultKafkaConfig)
      .stringStream(topicName)

    val query = Memory.memorySink(topicName).createDataStreamWriter(source).start()
    diagnosticQuery = Some(query)

    EmbeddedKafkaBroker.publishStringMessageToKafka(topicName, "test")

    eventually(timeout(Span(5, Seconds)), interval(Span(5, Millis))) {
      query.processAllAvailable()
      val currentContent = spark.table(topicName).collect().map(row => row.getAs[String]("value"))

      currentContent.shouldBe(Array("test")).
        withClue("Spark did not get diagnostic message from Kafka. Either one of them failed to start or they can't communicate.")
    }
    logger.info("Kafka and Spark are running, they are able to communicate")
  }
}

