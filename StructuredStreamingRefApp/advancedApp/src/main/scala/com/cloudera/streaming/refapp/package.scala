/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

package object refapp {

  private val logger = LoggerFactory.getLogger(getClass)

  val noop = {}

  /**
    * A Source creates a static or streaming DataFrame. Incoming data is parsed using the given schema.
    */
  type Source = StructType => DataFrame

  /**
    * Represents a streaming query that is executing continuously in the background as new data arrives
    */
  trait Query {
    def start(): Unit
    def stop(): Unit
    def restart(): Unit
    def processAllAvailable(): Unit
    val name: String
  }

  /**
    * Connects the output of a streaming query to a storage or messaging system.
    */
  trait Sink {
    val name: String
    def createDataStreamWriter(df: DataFrame): DataStreamWriter[Row]
  }

  // domain objects
  case class Transaction(transaction_id: String,
                         customer_id: Option[Int],
                         vendor_id: Option[Int],
                         event_state: Option[String],
                         event_timestamp: Timestamp,
                         price: Option[String],
                         card_type: Option[String])

  case class Vendor(vendor_id: Int,
                    vendor_name: String,
                    phone_number: String,
                    update_timestamp: Timestamp)

  case class Customer(customer_id: Int,
                      state_id: Int,
                      first_name: String,
                      last_name: String,
                      update_timestamp: Timestamp)

  case class State(state_id: Int,
                   state_name: String,
                   state_abbreviation: String)



  private val baseCheckpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString
  logger.info(s"Storing Spark checkpoints in $baseCheckpointLocation")

  def defaultCheckpointLocation(streamName: String) = s"$baseCheckpointLocation/$streamName"

}