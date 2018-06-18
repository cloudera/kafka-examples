/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * {@link Deserializer} implementation that converts byte arrays to objects of a subclass of {@link SpecificRecord}.
 * The following configuration is needed<ul>
 *     <li>{@code schemaProviderFactory=<factory_class_name>} for schema discovery</li>
 *     <li>{@code key.record.classname=<record_class_name>} or {@code value.record.classname=<record_class_name>}</li>
 * </ul>
 */
public class KafkaSpecificRecordDeserializer<T extends SpecificRecord> implements Deserializer<T> {


    public static final String KEY_RECORD_CLASSNAME = "key.record.classname";
    public static final String VALUE_RECORD_CLASSNAME = "value.record.classname";

    private SchemaProvider schemaProvider;

    private Schema readerSchema;


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String className = configs.get(isKey ? KEY_RECORD_CLASSNAME : VALUE_RECORD_CLASSNAME).toString();
        try {
            schemaProvider = SchemaUtils.getSchemaProvider(configs);
            Class<?> recordClass = Class.forName(className);
            this.readerSchema = new SpecificData(recordClass.getClassLoader()).getSchema(recordClass);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public T deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(data)) {

            int schemaId = readSchemaId(stream);
            VersionedSchema writerSchema = schemaProvider.get(schemaId);

            return readAvroRecord(stream, writerSchema.getSchema(), readerSchema);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int readSchemaId(InputStream stream) throws IOException {
        return new DataInputStream(stream).readInt();
    }

    private T readAvroRecord(InputStream stream, Schema writerSchema, Schema readerSchema) throws IOException {
        DatumReader<T> datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(stream, null);
        return datumReader.read(null, decoder);
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
