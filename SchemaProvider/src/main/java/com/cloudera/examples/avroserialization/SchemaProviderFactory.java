/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import java.util.Map;

public interface SchemaProviderFactory {
    public SchemaProvider getProvider(Map<String, ?> config) throws Exception;
}
