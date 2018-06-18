/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.*;
import java.util.Map;

/**
 * {@link Deserializer} implementation that converts byte arrays to {@link org.apache.avro.generic.GenericData.Record} objects.
 * The following configuration is needed<ul>
 *     <li>{@code schemaProviderFactory=<factory_class_name>} for schema discovery</li>
 *     <li>{@code schemaversion.<schema_name>=<schema_version>} for reader schema versions</li>
 * </ul>
 */
public class KafkaAvroDeserializer implements Deserializer<GenericData.Record> {

    private Map<String, VersionedSchema> readerSchemasByName;
    private SchemaProvider schemaProvider;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        schemaProvider = SchemaUtils.getSchemaProvider(configs);
        readerSchemasByName = SchemaUtils.getVersionedSchemas(configs, schemaProvider);
    }


    @Override
    public GenericData.Record deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(data)) {

            int schemaId = readSchemaId(stream);
            VersionedSchema writerSchema = schemaProvider.get(schemaId);

            VersionedSchema readerSchema = readerSchemasByName.get(writerSchema.getName());
            GenericData.Record avroRecord = readAvroRecord(stream, writerSchema.getSchema(), readerSchema.getSchema());
            return avroRecord;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int readSchemaId(InputStream stream ) throws IOException {
        try(DataInputStream is = new DataInputStream(stream)) {
            return is.readInt();
        }
    }

    private GenericData.Record readAvroRecord(InputStream stream, Schema writerSchema, Schema readerSchema) throws IOException {
        DatumReader<Object> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(stream, null);
        GenericData.Record record = new GenericData.Record(readerSchema);
        datumReader.read(record, decoder);
        return record;
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
