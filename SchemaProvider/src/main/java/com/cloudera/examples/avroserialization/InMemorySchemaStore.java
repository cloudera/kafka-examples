/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link SchemaStore} that keeps schemas in memory. Useful as a cache in other SchemasStore implementations.
 *
 * Stores {@link VersionedSchema} references in maps to enable fast lookup.
 */
public class InMemorySchemaStore implements SchemaStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemorySchemaStore.class);

    private final Map<Integer, VersionedSchema> schemasById = new ConcurrentHashMap<>();
    private final Map<SchemaNameWithVersion, VersionedSchema> schemasByNameAndVersion = new ConcurrentHashMap<>();
    private final Map<String, VersionedSchema> schemasByParsingForm = new ConcurrentHashMap<>();

    @Override
    public void add(VersionedSchema schema) {
        schemasById.put(schema.getId(), schema);
        schemasByNameAndVersion.put(new SchemaNameWithVersion(schema.getName(), schema.getVersion()), schema);
        schemasByParsingForm.put(SchemaNormalization.toParsingForm(schema.getSchema()), schema);
    }

    @Override
    public VersionedSchema get(int id) {
        VersionedSchema versionedSchema = schemasById.get(id);
        if (versionedSchema == null) {
            throw new RuntimeException("Could not find version with id=" + id);
        }
        return versionedSchema;
    }

    @Override
    public VersionedSchema get(String schemaName, int schemaVersion) {
        VersionedSchema versionedSchema = schemasByNameAndVersion.get(new SchemaNameWithVersion(schemaName, schemaVersion));
        if (versionedSchema == null) {
            throw new RuntimeException("Could not find version with name=" + schemaName + " and version=" + schemaVersion);
        }

        return versionedSchema;
    }

    @Override
    public VersionedSchema getMetadata(Schema schema) {
        String parsingForm = SchemaNormalization.toParsingForm(schema);
        VersionedSchema versionedSchema = schemasByParsingForm.get(parsingForm);
        if (versionedSchema == null) {
            throw new RuntimeException("Could not find metadata for schema.\nParsing form: " + parsingForm);
        }
        return versionedSchema;
    }

    public Collection<VersionedSchema> getAllSchemas() {
        return schemasById.values();
    }

    @Override
    public void close() {
        schemasById.clear();
        schemasByNameAndVersion.clear();
        schemasByParsingForm.clear();
    }

    private class SchemaNameWithVersion {
        private final String name;
        private final int version;

        SchemaNameWithVersion(String name, int version) {

            this.name = name;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaNameWithVersion that = (SchemaNameWithVersion) o;
            return version == that.version &&
                    Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, version);
        }

        @Override
        public String toString() {
            return "SchemaNameWithVersion{" +
                    "name='" + name + '\'' +
                    ", version=" + version +
                    '}';
        }
    }

}
