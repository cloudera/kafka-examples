/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.sql.Timestamp

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.execution.streaming.MemoryStream

class TransactionsFlowUnitTest extends UnitTestBase with BeforeAndAfter {
  import testImplicits._

  var transactionsFromStream: MemoryStream[Transaction] = _
  var transactiosnFlow: TransactionsFlow = _

  before {
    transactionsFromStream = MemoryStream[Transaction]
    transactiosnFlow = new TransactionsFlow(
      spark,
      statesFromCluster,
      customersFromCluster,
      vendorsFromCluster,
      transactionsFromStream = transactionsFromStream
        .toDF.withColumn("timestamp", $"event_timestamp".cast("timestamp")))
  }

  test("Valid records are written to the validTransactions output") {

    val validTransaction = Transaction(
      transaction_id = "1",
      customer_id = Some(1),
      vendor_id = Some(1),
      event_state = Some("CREATED"),
      event_timestamp = Timestamp.valueOf("2018-11-12 09:42:00"),
      price = Some("100"),
      card_type = Some("Credit"))

    testStream(transactiosnFlow.validTransactions.select('transaction_id, 'customer_id, 'vendor_id, 'event_state, 'event_timestamp, 'price, 'card_type)) (
      AddData(transactionsFromStream, validTransaction),
      CheckAnswer(validTransaction)
    )
  }

  test("Invalid records are written to the invalidTransactions output") {
    // Note: transactionsFlow.validTransactions and invalidTransactions contain the fields that we used for internal calculations, e.g. for validation
    // It enables us to check the internal calculations
    testStream(transactiosnFlow.invalidTransactions.select('transaction_id, 'valid_card_type)) (
      AddData(transactionsFromStream,
        Transaction(
          transaction_id = "2",
          customer_id = Some(1),
          vendor_id = Some(1),
          event_state = Some("CREATED"),
          event_timestamp = Timestamp.valueOf("2018-11-12 09:42:00"),
          price = Some("100"),
          card_type = Some("Invalid"))),
      CheckAnswer(("2", false))
    )
  }

}
