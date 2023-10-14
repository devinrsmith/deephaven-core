package io.deephaven.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.deephaven.chunk.ObjectChunksOneToOne;
import io.deephaven.functions.TypedFunction;
import io.deephaven.kafka.KafkaTools;
import io.deephaven.kafka.KafkaTools.Consume.KeyOrValueSpec;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.deephaven.avro.AvroSchemaDecoder.function;

public class Avro {
    public static KeyOrValueSpec of(Schema schema, int rowLimit) {
        final List<String> columnNames = new ArrayList<>(schema.getFields().size());
        final List<TypedFunction<? super AvroParsingState>> functions = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            columnNames.add(field.name());
            functions.add(function(field));
        }
        // note: we are relying on the functions impl to be *in* order. we should probably add that guarantee to docs.
        final ObjectChunksOneToOne<AvroParsingState> splayer = ObjectChunksOneToOne.of(functions);
        final ObjectChunksOneToOne<AvroParsingState> limited = rowLimit == Integer.MAX_VALUE
                ? splayer
                : ObjectChunksOneToOne.rowLimit(splayer, rowLimit);
        return KafkaTools.Consume.genericSpec(new AvroParsingStateDeser(), limited, columnNames);
    }

    public static KeyOrValueSpec of(String schemaName, int version, int rowLimit, SchemaRegistryClient client) throws RestClientException, IOException {
        return of(schema(client, schemaName, version), rowLimit);
    }

    public static KeyOrValueSpec of(String schemaName, int version, int rowLimit, Map<String, ?> configs) throws RestClientException, IOException {
        return of(schemaName, version, rowLimit, newSchemaRegistryClient(configs));
    }

    private static Schema schema(SchemaRegistryClient schemaClient, String schemaName, int version) throws IOException, RestClientException {
        final SchemaMetadata metadata = version > 0
                ? schemaClient.getSchemaMetadata(schemaName, version)
                : schemaClient.getLatestSchemaMetadata(schemaName);
        if (!AvroSchema.TYPE.equals(metadata.getSchemaType())) {
            throw new IllegalStateException(String.format("Expected schema type %s but was %s", AvroSchema.TYPE,
                    metadata.getSchemaType()));
        }
        final AvroSchema avroSchema = (AvroSchema) schemaClient.getSchemaBySubjectAndId(schemaName, metadata.getId());
        return avroSchema.rawSchema();
    }

    private static SchemaRegistryClient newSchemaRegistryClient(Map<String, ?> configs) {
        final AvroSchemaProvider provider = new AvroSchemaProvider();
        provider.configure(configs);
        final AbstractKafkaSchemaSerDeConfig config = new AbstractKafkaSchemaSerDeConfig(
                AbstractKafkaSchemaSerDeConfig.baseConfigDef(),
                configs);
        return new CachedSchemaRegistryClient(
                config.getSchemaRegistryUrls(),
                config.getMaxSchemasPerSubject(),
                List.of(provider),
                config.originalsWithPrefix(""),
                config.requestHeaders());
    }
}
