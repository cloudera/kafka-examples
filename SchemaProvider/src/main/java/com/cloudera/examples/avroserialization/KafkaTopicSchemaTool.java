/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package com.cloudera.examples.avroserialization;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class KafkaTopicSchemaTool {

    public static final String ADD = "add";
    public static final String LIST = "list";
    public static final String DESCRIBE = "describe";
    public static final String SCHEMA_FILE = "schema-file";
    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String ID = "id";
    public static final String SERVERS = "servers";
    public static final String TOPIC = "topic";
    public static final String CLIENT_CONFIGURATION = "client-configuration";
    private final KafkaTopicSchemaProvider provider;

    public KafkaTopicSchemaTool(KafkaTopicSchemaProvider provider) {
        this.provider = provider;
    }

    public Collection<VersionedSchema> listSchemas() {
        return provider.getAllSchemas();
    }

    public VersionedSchema getSchema(String name, int version) {
        return provider.get(name, version);
    }

    public VersionedSchema getSchema(int id) {
        return provider.get(id);
    }

    public VersionedSchema addSchema(String name, int version, String schemaFileName) throws Exception {
        int id = generateId();
        String avroSchema = new String(Files.readAllBytes(Paths.get(schemaFileName)));
        VersionedSchema versionedSchema = new VersionedSchema(id, name, version,
                new Schema.Parser().parse(avroSchema));
        provider.add(versionedSchema);
        return versionedSchema;
    }

    private int generateId() {
        Set<Integer> ids = listSchemas().stream().map(VersionedSchema::getId).collect(Collectors.toSet());
        int nextId = IntStream.iterate(0, i -> i + 1).filter(i -> !ids.contains(i)).findFirst().getAsInt();
        return nextId;
    }

    public static void main(String... args) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts(ADD);
        parser.accepts(LIST);
        parser.accepts(DESCRIBE);

        parser.accepts(SCHEMA_FILE).withRequiredArg();
        parser.accepts(NAME).withRequiredArg();
        parser.accepts(VERSION).withRequiredArg();
        parser.accepts(ID).withRequiredArg();

        parser.accepts(TOPIC).withRequiredArg();
        parser.accepts(SERVERS).withRequiredArg();
        parser.accepts(CLIENT_CONFIGURATION).withRequiredArg();

        try {


            OptionSet options = parser.parse(args);

            if (!(options.has(SERVERS) || options.has(CLIENT_CONFIGURATION))) {
                throw new IllegalStateException("You must specify '--" + CLIENT_CONFIGURATION + "' or '--" + SERVERS + "'.");
            }

            if (options.has(ADD)) {
                if (options.has(LIST) || options.has(DESCRIBE)) {
                    throw new IllegalArgumentException("You must specify exactly one of --"+ADD+", --"+LIST+" or --"+DESCRIBE+"");
                }
                if (!(options.has(SCHEMA_FILE) && options.has(NAME) && options.has(VERSION))) {
                    throw new IllegalStateException("You must specify all of --" + SCHEMA_FILE + ", --" + NAME + ", --" + VERSION + " when adding new schema.");
                }
                if (options.has(ID)) {
                    throw new IllegalStateException("You must not specify --" + ID + " when adding new schema.");
                }
                KafkaTopicSchemaTool tool = new KafkaTopicSchemaTool(createProvider(options));
                VersionedSchema schema = tool.addSchema(options.valueOf(NAME).toString(), Integer.valueOf(options.valueOf(VERSION).toString()), options.valueOf(SCHEMA_FILE).toString());
                System.out.println("Successfully added schema.");
                printSchema(schema);
            } else if (options.has(LIST)) {
                if (options.has(DESCRIBE)) {
                    throw new IllegalArgumentException("You must specify exactly one of --"+ADD+", --"+LIST+" or --"+DESCRIBE+"");
                }
                if (options.has(SCHEMA_FILE)) {
                    throw new IllegalStateException("You must not specify --" + SCHEMA_FILE + " when listing schemas.");
                }
                KafkaTopicSchemaTool tool = new KafkaTopicSchemaTool(createProvider(options));
                Stream<VersionedSchema> schemas = tool.listSchemas().stream();
                if (options.hasArgument(NAME)) {
                    schemas = schemas.filter(schema -> options.valueOf(NAME).equals(schema.getName()));
                }
                if (options.hasArgument(VERSION)) {
                    schemas = schemas.filter(schema -> options.valueOf(VERSION).equals(String.valueOf(schema.getVersion())));
                }
                if (options.hasArgument(ID)) {
                    schemas = schemas.filter(schema -> options.valueOf(ID).equals(String.valueOf(schema.getId())));
                }
                schemas.forEach(KafkaTopicSchemaTool::printSchema);
            }  else if (options.has(DESCRIBE)) {
                if (options.has(SCHEMA_FILE)) {
                    throw new IllegalStateException("You must not specify --" + SCHEMA_FILE + " when describing a schema.");
                }
                if (!options.has(ID) && !(options.has(NAME) && options.has(VERSION)) ) {
                    throw new IllegalStateException("You must either specify --" + ID + " or --" + NAME + " and --" + VERSION + ".");

                }
                KafkaTopicSchemaTool tool = new KafkaTopicSchemaTool(createProvider(options));
                VersionedSchema schema = options.has(ID)
                        ? tool.getSchema(Integer.valueOf(options.valueOf(ID).toString()))
                        : tool.getSchema(options.valueOf(NAME).toString(), Integer.valueOf(options.valueOf(VERSION).toString()));
                printSchema(schema);
                System.out.println("Avro schema:");
                System.out.println(schema.getSchema().toString(true));
            }
        } catch (Exception e) {
            e.printStackTrace();
            parser.printHelpOn(System.err);
        }

    }

    private static void printSchema(VersionedSchema schema) {
        System.out.println("ID: " + schema.getId() + "\tName: " + schema.getName() + "\tVersion: " + schema.getVersion());
    }

    private static KafkaTopicSchemaProvider createProvider(OptionSet options) throws Exception {
        Properties p = new Properties();
        if (options.has(CLIENT_CONFIGURATION)) {
            try(InputStream is = new FileInputStream(options.valueOf(CLIENT_CONFIGURATION).toString())) {
                p.load(is);
            }
        }
        if (options.has(SERVERS)) {
            p.put(KafkaTopicSchemaProvider.SCHEMA_PROVIDER_CONF_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(SERVERS));
        }
        if (options.has(TOPIC)) {
            p.put(KafkaTopicSchemaProvider.SCHEMA_TOPIC_NAME_CONF, options.valueOf(TOPIC));
        }
        if (!p.containsKey(KafkaTopicSchemaProvider.SCHEMA_PROVIDER_CONF_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                || !p.containsKey(KafkaTopicSchemaProvider.SCHEMA_TOPIC_NAME_CONF)) {
            throw new IllegalArgumentException("You must specify both --"+SERVERS+" and --"+TOPIC+", or add properties " +
                    KafkaTopicSchemaProvider.SCHEMA_PROVIDER_CONF_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG +
                    " and " + KafkaTopicSchemaProvider.SCHEMA_TOPIC_NAME_CONF + " to the file passed as --"+CLIENT_CONFIGURATION+".");
        }


        return (KafkaTopicSchemaProvider) new KafkaTopicSchemaProvider.KafkaTopicSchemaProviderFactory().getProvider(cast(p));

    }

    private static Map<String, Object> cast(Object o) {
        return (Map<String, Object>) o;
    }


}
