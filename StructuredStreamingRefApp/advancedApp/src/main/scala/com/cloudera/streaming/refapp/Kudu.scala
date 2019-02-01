/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Creates static Sources reading Kudu tables.
  */
class KuduSource(spark: SparkSession, master: String, database: String) {

  /**
    * Creates a Source that reads content from a table.
    * It will register a TempView with the same name in Spark session, so SQL queries can use it.
    */
  def loadTable(name: String)(ignored: StructType) = {
    val fullTableName = s"impala::$database.$name"
    val df = spark
      .read
      .options(
        Map(
          "kudu.master" -> master,
          "kudu.table" -> fullTableName)).kudu
    df.createOrReplaceTempView(name)
    df
  }
}

/**
  * Creates Sinks that produce streaming output to Kudu tables.
  *
  * @param checkpointLocation provides the path where the checkpoints are stored, given the name of the Sink
  */
class KuduSink(master: String, database: String, checkpointLocation: String => String) {

    def writeTable(sinkName: String, triggerSeconds: Int = 10) =
      new Sink {
        override def createDataStreamWriter(df: DataFrame): DataStreamWriter[Row] = {
          val fullTableName = s"impala::$database.$name"
          df
            .writeStream
            .format("kudu")
            .option("kudu.master", master)
            .option("kudu.table", fullTableName)
            .option("checkpointLocation", checkpointLocation(name))
            .option("retries", "3")
            .outputMode("update")
        }

        override val name: String = sinkName
      }

}
