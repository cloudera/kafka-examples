/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

public interface SchemaStore extends SchemaProvider {
    public void add(VersionedSchema schema) throws Exception;
}
