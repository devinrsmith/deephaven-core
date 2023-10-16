/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.deephaven.api.ColumnName;
import io.deephaven.functions.TypedFunction;
import io.deephaven.kafka.KafkaTools;
import io.deephaven.kafka.KafkaTools.Consume.KeyOrValueSpec;
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions.FieldPathToColumnName;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.protobuf.FieldPath;
import io.deephaven.protobuf.ProtobufDescriptorParser;
import io.deephaven.protobuf.ProtobufDescriptorParserOptions;
import io.deephaven.protobuf.ProtobufFunction;
import io.deephaven.protobuf.ProtobufFunctions;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Protobuf {

    /**
     * The kafka protobuf specs. This will fetch the {@link com.google.protobuf.Descriptors.Descriptor protobuf
     * descriptor} based on the {@link ProtobufConsumeOptions#descriptorProvider()} and create the
     * {@link com.google.protobuf.Message message} parsing functions according to
     * {@link io.deephaven.protobuf.ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)}. These
     * functions will be adapted to handle schema changes.
     *
     * @param options the options
     * @return the key or value spec
     * @see io.deephaven.protobuf.ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions) parsing
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec of(ProtobufConsumeOptions options) throws InvocationTargetException,
            NoSuchMethodException, IllegalAccessException, RestClientException, IOException {
        final Deserializer<? extends Message> deserializer;
        final Descriptor descriptor;
        if (options.descriptorProvider() instanceof DescriptorMessageClass) {
            final Class<? extends Message> clazz =
                    ((DescriptorMessageClass<? extends Message>) options.descriptorProvider()).clazz();
            descriptor = descriptor(clazz);
            // Note: using the explicit *parser*, and not the descriptor.
            deserializer = ProtobufDeserializers.of(options.protocol(), parser(clazz));
        } else if (options.descriptorProvider() instanceof DescriptorSchemaRegistry) {
            final DescriptorSchemaRegistry dsr = (DescriptorSchemaRegistry) options.descriptorProvider();
            final SchemaRegistryClient client = newSchemaRegistryClient(dsr.config());
            descriptor = descriptor(client, dsr);
            deserializer = ProtobufDeserializers.of(options.protocol(), descriptor);
        } else {
            throw new IllegalArgumentException("Unexpected DescriptorProvider");
        }
        final List<String> columnNames = new ArrayList<>();
        final List<TypedFunction<? super Message>> functions = new ArrayList<>();
        {
            final ProtobufFunctions pf = ProtobufDescriptorParser.parse(descriptor, options.parserOptions());
            final FieldPathToColumnName fieldPathToColumnName = options.pathToColumnName();
            final Map<FieldPath, Integer> indices = new HashMap<>();
            for (ProtobufFunction f : pf.functions()) {
                final int ix = indices.compute(f.path(), (fieldPath, i) -> i == null ? 0 : i + 1);
                final ColumnName columnName = fieldPathToColumnName.columnName(f.path(), ix);
                columnNames.add(columnName.name());
                functions.add(f.function());
            }
        }
        return KafkaTools.Consume.objectProcessorSpec(deserializer, ObjectProcessorFunctions.of(functions),
                columnNames);
    }

    private static Descriptor descriptor(Class<? extends Message> clazz)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Method getDescriptor = clazz.getMethod("getDescriptor");
        return (Descriptor) getDescriptor.invoke(null);
    }

    private static <T extends Message> Parser<T> parser(Class<T> clazz)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Method parser = clazz.getMethod("parser");
        // noinspection unchecked
        return (Parser<T>) parser.invoke(null);
    }

    private static Descriptor descriptor(SchemaRegistryClient registry, DescriptorSchemaRegistry dsr)
            throws RestClientException, IOException {
        final SchemaMetadata metadata = dsr.version().isPresent()
                ? registry.getSchemaMetadata(dsr.subject(), dsr.version().getAsInt())
                : registry.getLatestSchemaMetadata(dsr.subject());
        if (!ProtobufSchema.TYPE.equals(metadata.getSchemaType())) {
            throw new IllegalStateException(String.format("Expected schema type %s but was %s", ProtobufSchema.TYPE,
                    metadata.getSchemaType()));
        }
        final ProtobufSchema protobufSchema = (ProtobufSchema) registry
                .getSchemaBySubjectAndId(dsr.subject(), metadata.getId());
        // The potential need to set io.deephaven.kafka.protobuf.DescriptorSchemaRegistry#messageName
        // seems unfortunate; I'm surprised the information is not part of the kafka serdes protocol.
        // Maybe it's so that a single schema can be used, and different topics with different root messages can
        // all share that common schema?
        return dsr.messageName().isPresent()
                ? protobufSchema.toDescriptor(dsr.messageName().get())
                : protobufSchema.toDescriptor();
    }

    private static SchemaRegistryClient newSchemaRegistryClient(Map<String, String> configs) {
        // Note: choosing to not use the constructor with doLog which is a newer API; this is in support of downstream
        // users _potentially_ being able to replace kafka jars with previous versions.
        final AbstractKafkaSchemaSerDeConfig config = new AbstractKafkaSchemaSerDeConfig(
                AbstractKafkaSchemaSerDeConfig.baseConfigDef(),
                configs);
        // Note: choosing to not use SchemaRegistryClientFactory.newClient which is a newer API; this is in support of
        // downstream users _potentially_ being able to replace kafka jars with previous versions.
        return new CachedSchemaRegistryClient(
                config.getSchemaRegistryUrls(),
                config.getMaxSchemasPerSubject(),
                List.of(new ProtobufSchemaProvider()),
                config.originalsWithPrefix(""),
                config.requestHeaders());
    }
}
