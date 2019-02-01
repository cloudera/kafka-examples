/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp.kudu

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class KuduSinkUnitTest extends FunSuite with MockitoSugar {

  private val frame = mock[DataFrame]

  private def setupKuduContextMock(kuduContext: KuduContext, failTimes: Int): KuduContext = {
    if (failTimes > 0) {
      val stubber = doThrow(new RuntimeException)
      for (_ <- 2 to failTimes) {
        stubber.doThrow(new RuntimeException)
      }
      stubber.doCallRealMethod()
        .when(kuduContext).upsertRows(frame, "table")
    }
    kuduContext
  }

  test("kudu upsert fails, retries once") {
    val helper = new KuduSinkWithMockedContext(setupKuduContextMock(mock[KuduContext], failTimes = 1), 1)

    helper.sink.addBatch(0, frame)
    assert(helper.initialized == 1, "context should be initialized once")
  }

  test("kudu upsert fails twice, retries once, fails") {
    val helper = new KuduSinkWithMockedContext(setupKuduContextMock(mock[KuduContext], failTimes = 2), 1)

    intercept[RuntimeException] {
      helper.sink.addBatch(0, frame)
    }
    assert(helper.initialized == 1, "context should be initialized once")
  }

  test("kudu upsert fails 3 times, retries 3 times") {
    val helper = new KuduSinkWithMockedContext(setupKuduContextMock(mock[KuduContext], failTimes = 3), 3)
    helper.sink.addBatch(0, frame)
    assert(helper.initialized == 3, "context should be initialized three times")
  }

  test("kudu upsert fails 3 times, retries 4 times") {
    val helper = new KuduSinkWithMockedContext(setupKuduContextMock(mock[KuduContext], failTimes = 3), 4)
    helper.sink.addBatch(0, frame)
    assert(helper.initialized == 3, "context should be initialized only three times")
  }

}

class KuduSinkWithMockedContext(kuduContext: KuduContext, retries: Int) {

  // KuduSink constructor inits once
  var initialized = -1

  private def initKuduConext: KuduContext = {
    initialized += 1
    kuduContext
  }

  val sink = new KuduSink(initKuduConext, Map(
    "kudu.table" -> "table",
    "kudu.master" -> "master",
    "retries" -> retries.toString))
}
