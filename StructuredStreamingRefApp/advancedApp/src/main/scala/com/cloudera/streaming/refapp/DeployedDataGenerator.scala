/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

/**
  * Created by andrasbeni on 12/1/18.
  */
object DeployedDataGenerator {

  def main(args: Array[String]) {

    if (args.length != 2) {
      sys.error(
        """Usage:
          |com.cloudera.streaming.refapp.DeployedDataGenerator producer.config
          |producer.config path to kafka client properties
          |transactions.per.sec number of records produced to "transaction" topic per second
        """.stripMargin)
    }
    val Array(producerConfig, recordsPerSec) = args
    val kafkaConfig: KafkaConfig = KafkaConfig.fromPropertiesFile(producerConfig)

    CustomerGenerator(kafkaConfig, "customer").start()
    VendorGenerator(kafkaConfig, "vendor").start()
    TransactionGenerator(kafkaConfig, "transaction", recordsPerSec.toInt).start()

  }

}
