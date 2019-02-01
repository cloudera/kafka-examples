/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.sql.Timestamp
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, Serializer}
import org.slf4j.{Logger, LoggerFactory}

object CustomerGenerator {
  def apply(kafkaConfig: KafkaConfig, topic: String): DataGenerator[Integer, Customer] = new CustomerGenerator(kafkaConfig, topic).generator
}

object VendorGenerator {
  def apply(kafkaConfig: KafkaConfig, topic: String): DataGenerator[Integer, Vendor] = new VendorGenerator(kafkaConfig, topic).generator
}

object TransactionGenerator {
  def apply(kafkaConfig: KafkaConfig, topic: String, recordsPerSec: Int = 1): DataGenerator[Integer, Transaction] = new TransactionGenerator(kafkaConfig, topic, recordsPerSec).generator
}

class CustomerGenerator(kafkaConfig: KafkaConfig, topic: String) {
  val generator = new DataGenerator[Integer, Customer](
    kafkaConfig,
    topic = topic,
    createValue = Customer(
      customer_id = Random.nextInt(10) + 1,
      state_id = if (Random.nextInt(100) < 5) -1 else Random.nextInt(51) + 1,
      first_name = randomFirstName,
      last_name = randomLastName,
      update_timestamp = new Timestamp(System.currentTimeMillis() - Random.nextInt(48 * 60 * 60 * 1000))),
    getKey = _.customer_id,
    keySerializer = new IntegerSerializer(),
    valueSerializer = new CustomerSerializer())

  private val firstNames = Seq("John", "Jane", "Alex", "Jessica")

  private def randomFirstName = firstNames(Random.nextInt(firstNames.length))

  private val lastNames = Seq("Doe", "Smith", "Tailor", "Hamilton")

  private def randomLastName = lastNames(Random.nextInt(lastNames.length))
}

class VendorGenerator(kafkaConfig: KafkaConfig, topic: String) {
  val generator = new DataGenerator[Integer, Vendor](
    kafkaConfig,
    topic = topic,
    createValue = Vendor(
      vendor_id = Random.nextInt(10) + 1,
      vendor_name = randomVendorName,
      phone_number = randomPhoneNumber,
      update_timestamp = new Timestamp(System.currentTimeMillis() - Random.nextInt(48 * 60 * 60 * 1000))),
    getKey = _ => null,
    keySerializer = new IntegerSerializer(),
    valueSerializer = new VendorSerializer())

  private val vendorNames = Seq("Acme Corp.", "Cyberdyne Systems", "Hooli", "Initech", "Stark Industries", "Wayne Enterprises")

  private def randomVendorName = vendorNames(Random.nextInt(vendorNames.length))

  private def randomPhoneNumber = f"+1-${Random.nextInt(1000)}%03d-555-${Random.nextInt(10000)}%04d"
}

class TransactionGenerator(kafkaConfig: KafkaConfig, topic: String, recordsPerSecond: Int) {
  val generator = new DataGenerator[Integer, Transaction](
    kafkaConfig,
    topic = topic,
    createValue = Transaction(
      transaction_id = Random.alphanumeric.take(3).mkString,
      customer_id = if (Random.nextInt(100) < 20) None else Some(Random.nextInt(10) + 1),
      vendor_id = if (Random.nextInt(100) < 20) None else Some(Random.nextInt(10) + 1),
      event_state = randomEventState,
      event_timestamp = new Timestamp(System.currentTimeMillis() - Random.nextInt(60 * 1000)),
      price = if (Random.nextInt(100) < 20) None else Some(Random.nextInt(100000).toString),
      card_type = randomCardType),
    getKey = transaction => {
      val id: Integer = if (transaction.customer_id.isDefined) transaction.customer_id.get else null
      id
    },
    new IntegerSerializer(),
    new TransactionSerializer(),
    recordsPerSecond
  )

  private val states = Seq("CREATED", "SWIPED", "AUTHORIZED", "INVALID")

  private def randomEventState = if (Random.nextInt(100) < 20) None else Some(states(Random.nextInt(states.length)))

  private val cardTypes = Seq("Credit", "Debit", "Whatever")

  private def randomCardType = if (Random.nextInt(100) < 20) None else Some(cardTypes(Random.nextInt(cardTypes.length)))
}

class DataGenerator[K, V](kafkaConfig: KafkaConfig,
                          topic: String,
                          createValue: => V,
                          getKey: V => K,
                          keySerializer: Serializer[K],
                          valueSerializer: Serializer[V],
                          recordsPerSecond: Int = 1) {

  val logger : Logger = LoggerFactory.getLogger(getClass)

  def start(): Unit = {
    logger.info("Data generator starting")
    val config: Map[String, Object] = kafkaConfig.kafkaParams
    val producer = new KafkaProducer(config.asJava, keySerializer, valueSerializer)

    def generate(recordCount : Int) = try {
      for (_ <- 1 to recordCount) {
        val value: V = createValue
        val key: K = getKey(value)
        logger.debug(s"Producing to $topic: $value")
        producer.send(new ProducerRecord(topic, key, value))
      }
    } catch {
      case e: Exception =>
        logger.error("Exception while producing", e)
        System.exit(1)
    }

    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      def run(): Unit = generate(recordsPerSecond)
    }
    val sched = ex.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        logger.info("Data generator stopping")
        sched.cancel(false)
        producer.close()
        logger.info("Data generator stopped")
      }
    })
    logger.info("Data generator started")
  }
}
