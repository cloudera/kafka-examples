/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.io.File

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.slf4j.LoggerFactory


/**
  * Creates static and streaming Sources reading local or HDFS files.
  * Used for testing.
  */
class FileSources(spark: SparkSession, inputDir: String) {

  /**
    * Creates a streaming source that reads JSON files.
    * It assumes that the files are located in the inputDir/kafka/fileName directory
    */
  def jsonStream(fileName: String, timestampColumnName: String)(schema: StructType): DataFrame =
    spark.readStream
      .format("json")
      .option("maxFilesPerTrigger", "1")  // ensures that we have multiple minibatches: if we have more files each minibatch reads only one of them
      .schema(schema)
      .load(s"$inputDir/kafka/${fileName}")
      .repartition(1)
      .withColumn(Schemas.timestampColumnName, functions.col(timestampColumnName))

  import spark.implicits._

  /**
    * Creates a static source that reads JSON files.
    * It assumes that the files are located in the inputDir/cluster/fileName directory
    */
  def jsonFile(fileName: String)(schema: StructType): DataFrame =
    spark.sparkContext.textFile(s"$inputDir/cluster/${fileName}", 1).toDF.repartition(1)
      .select(functions.from_json('value, schema) as 'entity).select("entity.*")
}

/**
  * Creates Sinks that produce streaming output to CSV files.
  * Local or HDFS directory where the CSV files are written.
  *
  * @param outputDir
  * @param checkpointLocation provides the path where the checkpoints are stored, given the name of the Sink
  */
class FileSinks(outputDir: String, checkpointLocation: String => String) {

  private val logger = LoggerFactory.getLogger(getClass)

  def csv(sinkName: String, triggerSeconds: Int = 10) =
    new Sink {
      override def createDataStreamWriter(df: DataFrame): DataStreamWriter[Row] = {
        df
          .writeStream
          .outputMode(OutputMode.Append)
          .format("csv")
          .trigger(Trigger.ProcessingTime(s"$triggerSeconds seconds"))
          .option("checkpointLocation", checkpointLocation(name))
          .option("path", s"$outputDir/$name.csv")
          .option("header", "true")
      }

      override val name: String = sinkName
    }

  /**
    * Purges the output directory.
    */
  def cleanOutputs(): Unit = {
    val file = new File(outputDir)
    if (file.exists())
      file.listFiles().foreach {
        FileUtils.deleteDirectory
      }
    logger.info(s"Cleaned output directory $outputDir")
  }
}
