/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.ingest.FieldCopier;
import io.deephaven.kafka.ingest.FieldCopierAdapter;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.ingest.MultiFieldChunkAdapter;
import io.deephaven.protobuf.ProtobufFunctions;
import io.deephaven.protobuf.ProtobufOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.CommonTransform;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

class ProtobufImpl {
    static final class ProtobufConsume extends Consume.KeyOrValueSpec {

        private static final ObjectFunction<Object, Message> PROTOBUF_MESSAGE_OBJ = ObjectFunction.cast(Type.ofCustom(Message.class));

        private final ProtobufOptions options;
        private final String schemaSubject;
        private final int schemaVersion;

        ProtobufConsume(ProtobufOptions options, String schemaSubject, int schemaVersion) {
            this.options = Objects.requireNonNull(options);
            this.schemaSubject = Objects.requireNonNull(schemaSubject);
            this.schemaVersion = schemaVersion;
        }

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.of(new ProtobufSchemaProvider());
        }

        @Override
        Deserializer<?> getDeserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient, Map<String, ?> configs) {
            return new KafkaProtobufDeserializer<>(Objects.requireNonNull(schemaRegistryClient));
        }

        @Override
        KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient, Map<String, ?> configs, MutableInt nextColumnIndexMut, List<ColumnDefinition<?>> columnDefinitionsOut) {
            final Descriptor descriptor;
            try {
                descriptor = getDescriptor(schemaRegistryClient);
            } catch (RestClientException | IOException e) {
                throw new UncheckedDeephavenException(e);
            }
            final ProtobufFunctions functions = ProtobufFunctions.parse(descriptor, options);
            final List<FieldCopier> fieldCopiers = new ArrayList<>(functions.columns().size());
            final KeyOrValueIngestData data = new KeyOrValueIngestData();
            // arguably, others should be LinkedHashMap as well.
            data.fieldPathToColumnName = new LinkedHashMap<>();
            for (Entry<List<String>, TypedFunction<Message>> e : functions.columns().entrySet()) {
                final List<String> path = e.getKey();
                final TypedFunction<Message> function = e.getValue();
                final String columnName = String.join("_", path);
                data.fieldPathToColumnName.put(columnName, columnName);
                columnDefinitionsOut.add(ColumnDefinition.of(columnName, function.returnType()));
                fieldCopiers.add(FieldCopierAdapter.of(PROTOBUF_MESSAGE_OBJ.map(CommonTransform.of(function))));
            }
            // we don't have enough info at this time to create KeyOrValueProcessorImpl
            //data.extra = new KeyOrValueProcessorImpl(MultiFieldChunkAdapter.chunkOffsets(null, null), fieldCopiers, false);
            data.extra = fieldCopiers;
            return data;
        }

        @Override
        KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            return new KeyOrValueProcessorImpl(MultiFieldChunkAdapter.chunkOffsets(tableDef, data.fieldPathToColumnName), (List<FieldCopier>)data.extra, false);
        }

        private Descriptor getDescriptor(SchemaRegistryClient schemaRegistryClient) throws RestClientException, IOException {
            final SchemaMetadata metadata = schemaVersion > 0
                    ? schemaRegistryClient.getSchemaMetadata(schemaSubject, schemaVersion)
                    : schemaRegistryClient.getLatestSchemaMetadata(schemaSubject);
            if (!ProtobufSchema.TYPE.equals(metadata.getSchemaType())) {
                throw new IllegalStateException(String.format("Expected schema type %s but was %s", ProtobufSchema.TYPE, metadata.getSchemaType()));
            }
            // todo: we need to handle the dynamic case eventually, where protobuf descriptor is updated
            return ((ProtobufSchema) schemaRegistryClient.getSchemaBySubjectAndId(schemaSubject, metadata.getId())).toDescriptor();
        }
    }
}