/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.sql.Timestamp

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext

abstract class UnitTestBase extends StreamTest with BeforeAndAfter with SharedSQLContext {
  import testImplicits._

  lazy val statesFromCluster = List(
    State(state_id = 1, state_name = "Alabama", state_abbreviation = "AL"),
    State(state_id = 2, state_name = "Alaska", state_abbreviation = "AK")).toDF

  lazy val customersFromCluster = List(
    Customer(customer_id = 1, first_name = "John", last_name = "Doe", state_id = 1, update_timestamp = Timestamp.valueOf("2018-01-01 01:02:03")),
    Customer(customer_id = 2, first_name = "Jane", last_name = "Miller", state_id = 2, update_timestamp = Timestamp.valueOf("2018-01-02 01:02:03"))).toDF

  lazy val vendorsFromCluster = List(
    Vendor(vendor_id = 1, vendor_name = "Apple", phone_number = "123456", update_timestamp = Timestamp.valueOf("2018-11-13 01:02:03")),
    Vendor(vendor_id = 2, vendor_name = "Dell", phone_number = "345678", update_timestamp = Timestamp.valueOf("2018-11-13 01:02:03"))
  ).toDF

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

}
