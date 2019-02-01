/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EmbeddedSpark {

  val sparkSession: SparkSession = SparkSession.
    builder()
    .config(
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("test")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.app.id", UUID.randomUUID.toString))
    .getOrCreate()

}
