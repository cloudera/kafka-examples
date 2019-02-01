/*
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.time.Duration
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

case class Sources(statesFromCluster: Source,
                   customersFromCluster: Source,
                   vendorsFromCluster: Source,
                   customersFromStream: Source,
                   vendorsFromStream: Source,
                   transactionsFromStream: Source)

case class Sinks(invalidTransactions: Sink,
                 validTransactions: Sink,
                 customerOrphans: Sink,
                 vendorOrphans: Sink,
                 customers: Sink,
                 vendors: Sink,
                 transactionsOperationalMetadata: Sink)

class StreamingQueries(val invalidTransactions: Query,
                       val validTransactions: Query,
                       val customerOrphans: Query,
                       val vendorOrphans: Query,
                       val customers: Query,
                       val vendors: Query,
                       val transactionsOperationalMetadata: Query) {

  val allQueries = List(
    invalidTransactions,
    validTransactions,
    customerOrphans,
    vendorOrphans,
    customers,
    vendors,
    transactionsOperationalMetadata)

  def start(): Unit = {
    invalidTransactions.start()
    validTransactions.start()
    customerOrphans.start()
    vendorOrphans.start()
    customers.start()
    vendors.start()
    transactionsOperationalMetadata.start()
  }
}

class Application(spark: SparkSession,
                  sources: Sources,
                  sinks: Sinks,
                  queryRestartDurations: Map[String, Duration] = Map.empty,
                  cleanOutput: => Unit = noop,
                  clusterStartup: => Unit = noop,
                  initSources: => Unit = noop) {
  private val logger = LoggerFactory.getLogger(getClass)

  val transactionsFlow = new TransactionsFlow(
    spark,
    sources.statesFromCluster(Schemas.state),
    sources.customersFromCluster(Schemas.customer),
    sources.vendorsFromCluster(Schemas.vendor),
    sources.transactionsFromStream(Schemas.transaction)
  )

  val customersFlow = new CustomersFlow(spark, sources.customersFromStream(Schemas.customer))
  val vendorsFlow = new VendorsFlow(spark, sources.vendorsFromStream(Schemas.vendor))

  var streamingQueries = new StreamingQueries(
    // transactionsFlow.validTransactions and invalidTransactions contain columns used for internal calculations,
    // these do not fit to our output schemas
    createQuery(
      transactionsFlow.invalidTransactions.select("transaction_id", "customer_id", "vendor_id", "event_state", "event_timestamp", "price", "card_type"),
      sinks.invalidTransactions),
    createQuery(
      transactionsFlow.validTransactions.select("transaction_id", "customer_id", "vendor_id", "event_state", "event_timestamp", "price", "card_type"),
      sinks.validTransactions),
    createQuery(transactionsFlow.customerOrphans, sinks.customerOrphans),
    createQuery(transactionsFlow.vendorOrphans, sinks.vendorOrphans),
    createQuery(customersFlow.customers, sinks.customers),
    createQuery(vendorsFlow.vendors, sinks.vendors),
    createQuery(transactionsFlow.transactionsOperationalMetadata, sinks.transactionsOperationalMetadata)
  )

  def scheduleQueryRestarters(): Unit = {

    def restartQuery(query: Query): Unit = {
      println(s"Restarting query ${query.name}")
      try {
        query.restart()
      } catch {
        case e: Exception =>
          // log warn
          println(s"Could not restart query ${query.name}")
          e.printStackTrace()
      }
    }

    var schedules = List[ScheduledFuture[_]]()
    val executor = new ScheduledThreadPoolExecutor(1)

    def scheduleQueryRestarter(query:Query, period: Duration) = {
      println(s"Scheduling query restart of ${query.name} to $period")
      val task = new Runnable {
        def run(): Unit =  restartQuery(query)
      }
      val schedule = executor.scheduleAtFixedRate(task, period.getSeconds, period.getSeconds, TimeUnit.SECONDS)
      schedules = schedules :+ schedule
    }

    streamingQueries.allQueries.foreach{ query =>
      val queryRestartPeriod = queryRestartDurations.get(query.name)
      queryRestartPeriod match {
        case Some(period) => scheduleQueryRestarter(query, period)
        case None => // nothing to do
      }
    }

    if (schedules.nonEmpty) {
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() {
          schedules.foreach { sched =>
            sched.cancel(true)
          }
        }
      })
    }
  }

  def start() {
    logger.info("Application starting")
    clusterStartup
    cleanOutput
    initSources
    streamingQueries.start()
    scheduleQueryRestarters()
    logger.info("Application started")
  }

  def createQuery(dataFrame: DataFrame, sink: Sink) = new Query {
    val writer = sink.createDataStreamWriter(dataFrame)

    var streamingQuery: Option[StreamingQuery] = None

    def start(): Unit =
      streamingQuery = Some(writer.start())

    def stop(): Unit =
      streamingQuery.foreach{q => q.stop()}

    def restart(): Unit = {
      stop()
      start()
    }

    def processAllAvailable(): Unit =
      streamingQuery.foreach{q => q.processAllAvailable() }

    val name = sink.name
  }
}