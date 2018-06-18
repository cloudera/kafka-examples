/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import static com.cloudera.examples.avroserialization.KafkaTopicSchemaProvider.SCHEMA_PROVIDER_CONF_PREFIX;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 *
 * @param <T>
 */
class KafkaTopicStore<T> {

    public static final String SCHEMA_PROVIDER_PRODUCER_CONF_PREFIX = SCHEMA_PROVIDER_CONF_PREFIX + "producer.";
    public static final String SCHEMA_PROVIDER_CONSUMER_CONF_PREFIX = SCHEMA_PROVIDER_CONF_PREFIX + "consumer.";

    public static Map<String, Object> subConfig(Map<String, ?> config, String prefix) {
        Map<String, Object> filtered = config.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .collect(Collectors.toMap(
                        key -> key.substring(prefix.length()),
                        config::get));
        return filtered;
    }

    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicStore.class);

    private final String topic;
    private final Consumer<ConsumerRecord<Void, T>> newRecordConsumer;
    private Thread pollerThread;

    private final org.apache.kafka.clients.consumer.Consumer<Void, T> consumer;
    private final Producer<Void, T> producer;


    public KafkaTopicStore(
            Map<String, Object> config,
            String topic,
            String valueSerializerClassName,
            String valueDeserializerClassName,
            Consumer<ConsumerRecord<Void, T>> recordConsumer) {
        this.topic = topic;
        this.newRecordConsumer = recordConsumer;
        Properties consumerProperties = consumerProperties(config, valueDeserializerClassName);
        Properties producerProperties = producerProperties(config, valueSerializerClassName);

        consumer = new KafkaConsumer<>(consumerProperties);
        producer = new KafkaProducer<>(producerProperties);
    }

    private static Properties consumerProperties(Map<String, Object> config, String valueDeserializerClassName) {
        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
        props.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putAll(config);
        props.putAll(subConfig(config, SCHEMA_PROVIDER_CONSUMER_CONF_PREFIX));
        return props;
    }

    private static Properties producerProperties(Map<String, Object> config, String valueSerializerClassName) {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, "10");
        props.putAll(config);
        props.putAll(subConfig(config, SCHEMA_PROVIDER_PRODUCER_CONF_PREFIX));
        return props;
    }

    private void pollUntil(Predicate<ConsumerRecord<Void, T>> predicate) {
        try {
            while(true) {
                ConsumerRecords<Void, T> records = consumer.poll(1000);
                logger.debug("Read " + records.count() + " entries");
                for (ConsumerRecord<Void, T> record : records) {
                    logger.debug("Processing " + record.key() + " -> " + record.value());
                    newRecordConsumer.accept(record);
                    if (predicate.test(record)) {
                        return;
                    }
                }
            }
        } catch (InterruptException e) {
            logger.info("Interrupted while polling for new records.", e);
        } catch (Exception e) {
            logger.error("Caught error while polling for new records.", e);
        }
    }


    public void startAndCatchUp() {
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        logger.info("Start working with topic " + partitions);
        List<TopicPartition> topicPartitions =
                partitions.stream()
                        .map(p -> new TopicPartition(topic, p.partition()))
                        .collect(Collectors.toList());
        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);
        Map<TopicPartition, Long> initialOffsets = consumer.endOffsets(topicPartitions);
        logger.info("Offsets to catch up with: " +  initialOffsets);
        final Set<TopicPartition> partitionsCaughtUp = new HashSet<>();
        if (initialOffsets.values().stream().mapToLong(v -> v).sum() > 0) {
            Predicate<ConsumerRecord<Void, T>> allCaughtUp = r -> {
                // Checks
                TopicPartition topicPartition = new TopicPartition(topic, r.partition());
                logger.debug("Read partition, offset: " + r.partition() + ",  " + r.offset());
                if (r.offset() >= initialOffsets.get(topicPartition) - 1) {
                    partitionsCaughtUp.add(topicPartition);
                }
                return partitionsCaughtUp.size() == initialOffsets.size();
            };
            pollUntil(allCaughtUp);
        }
        pollerThread = new Thread(() -> pollUntil(r -> false), getClass().getSimpleName() + "-poller");
        pollerThread.start();
    }

    public void add(T record) throws Exception {
        producer.send(new ProducerRecord<>(topic, record)).get();
        // TODO maybe wait until record is read back
    }

    public void close() throws Exception {
        pollerThread.interrupt();
        pollerThread.join(1000);
    }




}
