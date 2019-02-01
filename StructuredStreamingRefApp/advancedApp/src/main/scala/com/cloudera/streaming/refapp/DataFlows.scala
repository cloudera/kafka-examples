/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
  * Enriches incoming customer flow with state name and abbreviation.
  */
class CustomersFlow(spark: SparkSession, customersFromStream: DataFrame) {
  private val customersWithWatermark = customersFromStream.withWatermark(Schemas.timestampColumnName, "10 seconds")
  customersWithWatermark.createOrReplaceTempView("customers_from_kafka")

  val customers = spark.sql(
    """SELECT customer_id, first_name, last_name, state_name,
           state_abbreviation, CAST(update_timestamp AS TIMESTAMP) update_timestamp
         FROM customers_from_kafka c
         LEFT OUTER JOIN states_from_cluster s
         ON c.state_id = s.state_id""")
}

/**
  * No transformation
  */
class VendorsFlow(spark: SparkSession, vendorsFromStream: DataFrame) {
  private val vendorsWithWatemark = vendorsFromStream.withWatermark(Schemas.timestampColumnName, "10 seconds")
  vendorsWithWatemark.createOrReplaceTempView("vendors_from_kafka")

  val vendors = spark.sql(
    """SELECT vendor_id, vendor_name, phone_number,
         CAST(update_timestamp AS TIMESTAMP) update_timestamp
         FROM vendors_from_kafka""")
}

/**
  * Processes incoming transactions: validates, finds customer and vendor orphans and produces
  * operational metadata.
  */
class TransactionsFlow(spark: SparkSession,
                       statesFromCluster: DataFrame, // Schemas.states
                       customersFromCluster: DataFrame, // Schemas.customer
                       vendorsFromCluster: DataFrame, // Schemas.vendor
                       transactionsFromStream: DataFrame) {

  import spark.implicits._

  private val transactionsWithWatemark = transactionsFromStream.withWatermark(Schemas.timestampColumnName, "10 seconds")

  statesFromCluster.createOrReplaceTempView("states_from_cluster")
  customersFromCluster.createOrReplaceTempView("customers_from_cluster")
  vendorsFromCluster.createOrReplaceTempView("vendors_from_cluster")
  transactionsWithWatemark.createOrReplaceTempView("transactions")


  // TODO consider eliminating unnecessary columns or rewrite to use pure SQL
  // timestamp check is not needed if the event comes from kafka
  private val validatedTransactions = transactionsWithWatemark.withColumn("mandatory_fields_exist", !'customer_id.isNull && !$"vendor_id".isNull &&
    !$"event_timestamp".isNull)
    .withColumn("valid_card_type", 'card_type.isin("Credit", "Debit"))
    .withColumn("valid_event_state", 'event_state.isin("CREATED", "SWIPED", "CANCELLED", "SIG_REQD", "AUTHORIZED", "DECLINED"))
    .withColumn("parsed_event_timestamp", functions.to_timestamp('event_timestamp, "yyyy-mm-dd"))
    .withColumn("correct_timestamp_format", !'parsed_event_timestamp.isNull)
    .withColumn("valid_record", 'mandatory_fields_exist && 'valid_card_type && 'valid_event_state && 'correct_timestamp_format)

  /**
    * Transaction records with missing / incorrect data
    */
  val invalidTransactions = validatedTransactions.filter(!'valid_record)

  /**
    * Transactions with complete and correct data
    */
  val validTransactions = validatedTransactions.filter('valid_record)
  validTransactions.createOrReplaceTempView("card_transactions_good_records")

  /**
    * Customers that did not exist in our database but were referenced in transactions
    */
  val customerOrphans = spark.sql(
    """SELECT customer_id, 'Unknown' first_name, 'Unknown' last_name,
          -1 state_id, CURRENT_TIMESTAMP() update_timestamp
        FROM
          (SELECT customer_id FROM card_transactions_good_records) sc
        LEFT ANTI JOIN
          (SELECT customer_id FROM customers_from_cluster) cc
        USING (customer_id)""").dropDuplicates("customer_id")

  customerOrphans.createOrReplaceTempView("customer_orphans")

  /**
    * Customers that did not exist in our database but were referenced in transactions
    */
  val vendorOrphans = spark.sql(
    """SELECT vendor_id, 'Unknown' vendor_name, 'Unknown' phone_number,
          CURRENT_TIMESTAMP() update_timestamp
        FROM
          (SELECT vendor_id FROM card_transactions_good_records) sv
        LEFT ANTI JOIN
          (SELECT vendor_id FROM vendors_from_cluster) cv
          USING (vendor_id)""").dropDuplicates("vendor_id")

  vendorOrphans.createOrReplaceTempView("vendor_orphans")

  // TODO add more operational metadata for invalid records, customer and vendor orphans
  /**
    * Operational metadata for monitoring.
    */
  val transactionsOperationalMetadata = transactionsWithWatemark
    .groupBy(functions.window(functions.col(Schemas.timestampColumnName), "1 minutes"))
    .count().as("c")
    .selectExpr("c.window.start as start_ts", "c.window.end as end_ts", "c.count as num_transactions")

}
