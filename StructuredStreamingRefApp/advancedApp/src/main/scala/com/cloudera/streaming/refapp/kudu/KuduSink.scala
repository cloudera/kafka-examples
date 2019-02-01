/*
 * Modifications copyright (C) 2019 Cloudera Inc
 */
package com.cloudera.streaming.refapp.kudu

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object KuduSink {
  def withDefaultContext(sqlContext: SQLContext, parameters: Map[String, String]) =
    new KuduSink(new KuduContext(parameters("kudu.master"), sqlContext.sparkContext), parameters)
}

/**
  * A simple Structured Streaming sink which writes the data frame to Kudu.
  * It preserves exactly once semantics, as it's idempotent in the face of
  * multiple attempts to add the same batch.
  *
  * It uses the following parameters:
  * kudu.master - host:port pair of a kudu master node
  * kudu.table - full table name
  * checkpointLocation - where the checkpoint will be stored
  */
class KuduSink(initKuduContext: => KuduContext, parameters: Map[String, String]) extends Sink {

  private val logger = LoggerFactory.getLogger(getClass)

  private var kuduContext = initKuduContext

  private val tablename = parameters("kudu.table")

  private val retries = parameters.getOrElse("retries", "1").toInt
  require(retries >= 0, "retries must be non-negative")

  logger.info(s"Created Kudu sink writing to table $tablename")

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    for (attempt <- 0 to retries) {
      try {
        kuduContext.upsertRows(data, tablename)
        return
      } catch {
        case NonFatal(e) =>
          if (attempt < retries) {
            logger.warn("Kudu upsert error, retrying...", e)
            kuduContext = initKuduContext
          }
          else {
            logger.error("Kudu upsert error, exhausted", e)
            throw e
          }
      }
    }
  }
}
