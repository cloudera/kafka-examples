/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

object EmbeddedKafkaBroker {

  def embeddedKafkaConfig = EmbeddedKafkaConfig.defaultConfig

  def defaultKafkaConfig =  KafkaConfig.fromBrokerList(
    s"localhost:${embeddedKafkaConfig.kafkaPort}")

  def start() {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        EmbeddedKafkaBroker.stop()
      }
    })

    EmbeddedKafka.start()
  }

  def stop() { EmbeddedKafka.stop()}

  def publishStringMessageToKafka(topic: String, message: String) { EmbeddedKafka.publishStringMessageToKafka(topic, message) }
}
