/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import org.apache.avro.Schema;

public interface SchemaProvider extends AutoCloseable {
    public VersionedSchema get(int id);
    public VersionedSchema get(String schemaName, int schemaVersion);
    public VersionedSchema getMetadata(Schema schema);
}
