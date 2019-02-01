/*
 * Modifications copyright (C) 2019 Cloudera Inc
 */
package com.cloudera.streaming.refapp.kudu

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

/**
  * Registers KuduSink to Spark streaming, so it can be used with format "kudu".
  *
  * Note: to make it effective you need META-INF/services/org.apache.spark.sql.sources.DataSourceRegister to
  * refer to this class.
  */
class KuduSinkProvider extends StreamSinkProvider with DataSourceRegister {

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    require(outputMode == OutputMode.Update, "only 'update' OutputMode is supported")
    KuduSink.withDefaultContext(sqlContext, parameters)
  }

  override def shortName(): String = "kudu"
}