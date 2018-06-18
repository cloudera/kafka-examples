/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class KafkaTopicStoreUsage {

    private static final String KAFKA_CLUSTER = System.getProperty("INTEGRATION_TEST_CLUSTER");
    private static final String TOPIC = "avro-example";
    public static final String SCHEMAPROVIDER_TOPIC_NAME = "__com_cloudera_schemaprovider";

    /*
     kafka-topics --create --topic __com_cloudera_schemaprovider --partitions 1 --replication-factor 3 --config min.insync.replicas=2  --config retention.ms=-1 --config retention.bytes=-1 --zookeeper $(hostname):2181
     kafka-topics --create --topic avro-example                  --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --zookeeper $(hostname):2181
    */

    public void add() throws Exception {
        KafkaTopicSchemaTool.main("--add", "--name", "user", "--version", "1",
                "--schema-file", "src/main/avro/user_v1_1.avsc", "--servers", KAFKA_CLUSTER, "--topic", SCHEMAPROVIDER_TOPIC_NAME);
    }



    public void produce() throws Exception {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaSpecificRecordSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
        producerProps.put(KafkaSpecificRecordSerializer.VALUE_RECORD_CLASSNAME, User.class.getName());
        producerProps.put(SchemaUtils.SCHEMA_PROVIDER_FACTORY_CONFIG, KafkaTopicSchemaProvider.KafkaTopicSchemaProviderFactory.class.getName());
        producerProps.put(KafkaTopicSchemaProvider.SCHEMA_TOPIC_NAME_CONF, SCHEMAPROVIDER_TOPIC_NAME);
        producerProps.put(KafkaTopicSchemaProvider.SCHEMA_PROVIDER_CONF_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);

        KafkaProducer<Integer, User> producer = new KafkaProducer<>(producerProps);

        for (User u : new User[] {
                new User("user1", "User, First", 0L),
                new User("user2", "User, Second", 10000L),
                new User("user3", "User, Name Third", 20000L)
        })
            producer.send(new ProducerRecord<>(TOPIC, u.getIdentifier().hashCode(), u)).get();
    }


    public void consume() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSpecificRecordDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, new Object().hashCode() + "");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KafkaSpecificRecordDeserializer.VALUE_RECORD_CLASSNAME, User2.class.getName());
        consumerProps.put(SchemaUtils.SCHEMA_PROVIDER_FACTORY_CONFIG, KafkaTopicSchemaProvider.KafkaTopicSchemaProviderFactory.class.getName());
        consumerProps.put(KafkaTopicSchemaProvider.SCHEMA_TOPIC_NAME_CONF, SCHEMAPROVIDER_TOPIC_NAME);
        consumerProps.put(KafkaTopicSchemaProvider.SCHEMA_PROVIDER_CONF_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);

        KafkaConsumer<Integer, User2> consumer = new KafkaConsumer<>(consumerProps);

        consumer.subscribe(Collections.singletonList(TOPIC));
        while(true) {
            consumer.poll(1000).forEach(r -> {
                User2 u = r.value();
                System.out.println(u);
            });
        }

    }
}
