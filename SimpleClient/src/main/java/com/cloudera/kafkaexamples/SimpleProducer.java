/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.kafkaexamples;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {
  public static void main(String[] args) {
    long events = Long.parseLong(args[0]);
    long ufoId = Math.round(Math.random() * Integer.MAX_VALUE);
 
    // Set up Java properties
    Properties props = new Properties();
    // This should point to at least one broker. Some communication
    // will occur to find the controller. Adding more brokers will
    // help in case of host failure or broker failure.
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        "hostname1:port1,hostname2:port2,hostname3:port3");
    // Enable a few useful properties for this example. Use of these
    // settings will depend on your particular use case.
    props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    // Required properties to process records
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    try {
      for (long nEvents = 0; nEvents < events; nEvents++) {
        String key = Long.toString(ufoId++);
        long runtime = new Date().getTime();
        double latitude = (Math.random() * (2 * 85.05112878)) - 85.05112878;
        double longitude = (Math.random() * 360.0) - 180.0;
        String msg = runtime + "," + latitude + "," + longitude;
        ProducerRecord<String, String> data = new ProducerRecord<String, String>("ufo_sightings", key, msg);
        producer.send(data);
        long wait = Math.round(Math.random() * 25);
        Thread.sleep(wait);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.close();
    }
  }
}
