/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import com.cloudera.kafkaexample.avro.SerializedVersionedSchema;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class KafkaTopicSchemaProvider implements SchemaStore {

    public static final String SCHEMA_PROVIDER_CONF_PREFIX = "schemaprovider.";
    public static final String SCHEMA_TOPIC_NAME_CONF = SCHEMA_PROVIDER_CONF_PREFIX + "topic";

    public static final class KafkaTopicSchemaProviderFactory implements SchemaProviderFactory {

        @Override
        public SchemaProvider getProvider(Map<String, ?> config) throws Exception {
            InMemorySchemaStore cache = new InMemorySchemaStore();
            String topic = config.get(SCHEMA_TOPIC_NAME_CONF).toString();
            Map<String, Object> storeConfig = KafkaTopicStore.subConfig(config, SCHEMA_PROVIDER_CONF_PREFIX);

            KafkaTopicStore<VersionedSchema> store = new KafkaTopicStore<>(
                    storeConfig,
                    topic,
                    VersionedSchemaSerializer.class.getName(),
                    VersionedSchemaDeserializer.class.getName(),
                    r -> cache.add(r.value()));
            store.startAndCatchUp();
            return new KafkaTopicSchemaProvider(cache, store);
        }
    }



    public static class VersionedSchemaDeserializer implements Deserializer<VersionedSchema> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public VersionedSchema deserialize(String topic, byte[] data) {
            try {
                DatumReader<SerializedVersionedSchema> datumReader = new SpecificDatumReader<>(SerializedVersionedSchema.class);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                SerializedVersionedSchema serialized = datumReader.read(null, decoder);
                VersionedSchema schema = new VersionedSchema(
                        serialized.getId(),
                        serialized.getName().toString(),
                        serialized.getVersion(),
                        new Schema.Parser().parse(serialized.getAvroSchema().toString()));
                return schema;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {

        }
    }

    public static class VersionedSchemaSerializer implements Serializer<VersionedSchema> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, VersionedSchema data) {
            try {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                DatumWriter<SerializedVersionedSchema> datumWriter = new SpecificDatumWriter<>(SerializedVersionedSchema.class);
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
                SerializedVersionedSchema s = new SerializedVersionedSchema(data.getId(), data.getName(), data.getVersion(), data.getSchema().toString(true));
                datumWriter.write(s, encoder);
                encoder.flush();
                byte[] serialized = stream.toByteArray();
                return serialized;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {

        }
    }


    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicSchemaProvider.class);

    private final InMemorySchemaStore cache;

    private final KafkaTopicStore<VersionedSchema> store;

    public KafkaTopicSchemaProvider(InMemorySchemaStore cache, KafkaTopicStore<VersionedSchema> store) {
        this.store = store;
        this.cache = cache;
    }

    @Override
    public VersionedSchema get(int id) {
        return cache.get(id);
    }

    @Override
    public VersionedSchema get(String schemaName, int schemaVersion) {
        return cache.get(schemaName, schemaVersion);
    }

    @Override
    public VersionedSchema getMetadata(Schema schema) {
        return cache.getMetadata(schema);
    }

    public Collection<VersionedSchema> getAllSchemas() {
        return cache.getAllSchemas();
    }

    public void add(VersionedSchema schema) throws Exception {
        store.add(schema);
    }

    @Override
    public void close() throws Exception {
        cache.close();
        try {
            store.close();
        } catch (Exception e) {
            logger.error("Could not close store.", e);
        }
    }
}
