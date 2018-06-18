/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * {@link Serializer} implementation that converts objects of a subclass of {@link SpecificRecord} to byte arrays.
 * The following configuration is needed<ul>
 *     <li>{@code schemaProviderFactory=<factory_class_name>} for schema discovery</li>
 *     <li>{@code key.record.classname=<record_class_name>} or {@code value.record.classname=<record_class_name>}</li>
 * </ul>
 */
public class KafkaSpecificRecordSerializer<T extends SpecificRecord> implements Serializer<T> {

    public static final String KEY_RECORD_CLASSNAME = "key.record.classname";
    public static final String VALUE_RECORD_CLASSNAME = "value.record.classname";

    private int writerSchemaId;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String className = configs.get(isKey ? KEY_RECORD_CLASSNAME : VALUE_RECORD_CLASSNAME).toString();
        try (SchemaProvider schemaProvider = SchemaUtils.getSchemaProvider(configs)) {
            Class<?> recordClass = Class.forName(className);
            Schema writerSchema = new SpecificData(recordClass.getClassLoader()).getSchema(recordClass);
            this.writerSchemaId = schemaProvider.getMetadata(writerSchema).getId();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {

            writeSchemaId(stream, writerSchemaId);
            writeSerializedAvro(stream, data);

            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize data", e);
        }
    }

    private void writeSchemaId(ByteArrayOutputStream stream, int id) throws IOException {
        try( DataOutputStream os = new DataOutputStream(stream)) {
            os.writeInt(id);
        }
    }

    private void writeSerializedAvro(ByteArrayOutputStream stream, T data) throws IOException {
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        DatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
        datumWriter.write(data, encoder);
        encoder.flush();
    }

    @Override
    public void close() {
        // nothing to do
    }

}
