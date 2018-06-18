/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
/**
 * {@link Serializer} implementation that converts byte arrays to {@link org.apache.avro.generic.GenericData.Record} objects.
 * The following configuration is needed<ul>
 *     <li>{@code schemaProviderFactory=<factory_class_name>} for schema discovery</li>
 *     <li>{@code schemaversion.<schema_name>=<schema_version>} for reader schema versions</li>
 * </ul>
 */
public class KafkaAvroSerializer<T extends GenericContainer> implements Serializer<T> {

    private SchemaProvider schemaProvider;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        schemaProvider = SchemaUtils.getSchemaProvider(configs);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            VersionedSchema schema = getSchema(data, topic);

            writeSchemaId(stream, schema.getId());
            writeSerializedAvro(stream, data, schema.getSchema());

            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize data", e);
        }
    }

    private void writeSchemaId(ByteArrayOutputStream stream, int id) throws IOException {
        try (DataOutputStream os = new DataOutputStream(stream)) {
            os.writeInt(id);
        }
    }

    private void writeSerializedAvro(ByteArrayOutputStream stream, T data, Schema schema) throws IOException {
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        DatumWriter<T> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.write(data, encoder);
        encoder.flush();
    }

    private VersionedSchema getSchema(T data, String topic) {
        return schemaProvider.getMetadata( data.getSchema());
    }

    @Override
    public void close() {
        try {
            schemaProvider.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
