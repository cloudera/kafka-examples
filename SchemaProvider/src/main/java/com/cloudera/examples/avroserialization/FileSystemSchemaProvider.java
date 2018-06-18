/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Example of {@link SchemaProvider} implementation with a backend different form Kafka.
 *
 * This object is configured with a directory name.
 * The directory is monitored for change and on any event, all schemas are re-read.
 * Schema name, version and identifier are parsed from file names.
 */
public class FileSystemSchemaProvider implements SchemaProvider {

    public static class FileSystemSchemaProviderFactory implements SchemaProviderFactory {

        public static final String SCHEMA_DIR_CONFIG = "schema.directory";

        @Override
        public SchemaProvider getProvider(Map<String, ?> config) throws Exception {
            return new FileSystemSchemaProvider(config.get(SCHEMA_DIR_CONFIG).toString());
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(FileSystemSchemaProvider.class);

    private static final Pattern SCHEMA_FILENAME_PATTERN = Pattern.compile("([a-zA-Z0-9_.]+)_v(\\d+)_(\\d+)\\.avsc");
    private final String schemaDirectory;
    private final Thread watcherThread;
    private volatile InMemorySchemaStore cache;



    public FileSystemSchemaProvider(String schemaDirectory) throws Exception {
        this.schemaDirectory = schemaDirectory;
        watcherThread = new Thread(new PathWatcherRunnable(schemaDirectory));
        watcherThread.start();
        rereadSchemas();

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

    @Override
    public void close() throws Exception {
        watcherThread.interrupt();
        watcherThread.join(1000);
        cache.close();
    }

    private void rereadSchemas() {
        // Re-read the whole directory for simplicity.
        InMemorySchemaStore newCache = new InMemorySchemaStore();
        File[] schemaFiles = new File(schemaDirectory)
                .listFiles((dir, name) -> SCHEMA_FILENAME_PATTERN.matcher(name).matches());
        if (schemaFiles == null) {
            throw new RuntimeException("Could not list schema directory " + schemaDirectory);
        }
        Arrays.stream(schemaFiles)
                .map(this::readSchemaFile)
                .forEach(newCache::add);
        this.cache = newCache;
    }

    private VersionedSchema readSchemaFile(File schemaFile) {
        // Parse file metadata from file name.
        Matcher matcher = SCHEMA_FILENAME_PATTERN.matcher(schemaFile.getName());
        matcher.matches();
        String name = matcher.group(1);
        int version = Integer.parseInt(matcher.group(2));
        int id = Integer.parseInt(matcher.group(3));
        try {
            Schema schema = new Schema.Parser().parse(schemaFile);
            return new VersionedSchema(id, name, version, schema);
        } catch (IOException e) {
            throw new RuntimeException("Could not parse schema file " + schemaFile, e);
        }
    }

    private class PathWatcherRunnable implements Runnable {

        private final WatchService watcher;

        PathWatcherRunnable(String schemaDirectory) throws Exception {
            watcher = Paths.get(schemaDirectory).getFileSystem().newWatchService();

        }

        @Override
        public void run() {
            // poll the FS for changes until interrupted
            while (true) {
                try {
                    watcher.take();
                    rereadSchemas();
                } catch (InterruptedException e) {
                    logger.info("Interrupted while polling for file system changes.", e);
                    return;
                } catch (Exception e) {
                    logger.error("Error while reading schemas.", e);
                }
            }
        }
    }
}
