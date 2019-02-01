/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.{DataFrame, Row}

object Memory {

  def memorySink(sinkName: String) = new Sink {
    override def createDataStreamWriter(df: DataFrame): DataStreamWriter[Row] = df
      .writeStream
      .outputMode(OutputMode.Append)
      .queryName(name)
      .format("memory")

    override val name: String = sinkName
  }

}
