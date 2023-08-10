/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
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
import io.deephaven.protobuf.ProtobufFunction;
import io.deephaven.protobuf.ProtobufFunctions;
import io.deephaven.protobuf.ProtobufFunctions.Builder;
import io.deephaven.protobuf.ProtobufOptions;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.qst.type.Type.Visitor;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.BoxTransform;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.CommonTransform;
import io.deephaven.stream.blink.tf.DhNullableTypeTransform;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.NullFunctions;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.PrimitiveFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.stream.blink.tf.UnboxTransform;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

class ProtobufImpl {

    @VisibleForTesting
    static ProtobufFunctions schemaChangeAwareFunctions(Descriptor descriptor, ProtobufOptions options) {
        return new ParsedStates(descriptor, options).functionsForSchemaChanges();
    }

    static final class ProtobufConsume extends Consume.KeyOrValueSpec {

        private static final ObjectFunction<Object, Message> PROTOBUF_MESSAGE_OBJ =
                ObjectFunction.cast(Type.ofCustom(Message.class));

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
        Deserializer<?> getDeserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            return new KafkaProtobufDeserializer<>(Objects.requireNonNull(schemaRegistryClient));
        }

        @Override
        KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs, MutableInt nextColumnIndexMut, List<ColumnDefinition<?>> columnDefinitionsOut) {
            final Descriptor descriptor;
            try {
                descriptor = getDescriptor(schemaRegistryClient);
            } catch (RestClientException | IOException e) {
                throw new UncheckedDeephavenException(e);
            }
            final ProtobufFunctions functions = schemaChangeAwareFunctions(descriptor, options);
            final List<FieldCopier> fieldCopiers = new ArrayList<>(functions.functions().size());
            final KeyOrValueIngestData data = new KeyOrValueIngestData();
            // arguably, others should be LinkedHashMap as well.
            data.fieldPathToColumnName = new LinkedHashMap<>();
            for (ProtobufFunction f : functions.functions()) {
                final TypedFunction<Message> transformedFunction = CommonTransform.of(f.function());
                final String columnName =
                        f.path().stream().map(FieldDescriptor::getName).collect(Collectors.joining("_"));
                data.fieldPathToColumnName.put(columnName, columnName);
                columnDefinitionsOut.add(ColumnDefinition.of(columnName, f.function().returnType()));
                fieldCopiers.add(FieldCopierAdapter.of(PROTOBUF_MESSAGE_OBJ.map(transformedFunction)));
            }
            // we don't have enough info at this time to create KeyOrValueProcessorImpl
            // data.extra = new KeyOrValueProcessorImpl(MultiFieldChunkAdapter.chunkOffsets(null, null), fieldCopiers,
            // false);
            data.extra = fieldCopiers;
            return data;
        }

        @Override
        KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            return new KeyOrValueProcessorImpl(
                    MultiFieldChunkAdapter.chunkOffsets(tableDef, data.fieldPathToColumnName),
                    (List<FieldCopier>) data.extra, false);
        }

        private Descriptor getDescriptor(SchemaRegistryClient schemaRegistryClient)
                throws RestClientException, IOException {
            final SchemaMetadata metadata = schemaVersion > 0
                    ? schemaRegistryClient.getSchemaMetadata(schemaSubject, schemaVersion)
                    : schemaRegistryClient.getLatestSchemaMetadata(schemaSubject);
            if (!ProtobufSchema.TYPE.equals(metadata.getSchemaType())) {
                throw new IllegalStateException(String.format("Expected schema type %s but was %s", ProtobufSchema.TYPE,
                        metadata.getSchemaType()));
            }
            // todo: we need to handle the dynamic case eventually, where protobuf descriptor is updated
            return ((ProtobufSchema) schemaRegistryClient.getSchemaBySubjectAndId(schemaSubject, metadata.getId()))
                    .toDescriptor();
        }
    }

    private static ProtobufFunctions withBestTypes(ProtobufFunctions functions) {
        final Builder builder = ProtobufFunctions.builder();
        for (ProtobufFunction f : functions.functions()) {
            builder.addFunctions(ProtobufFunction.of(f.path(), withBestTypes(f.function())));
        }
        return builder.build();
    }

    private static <X> TypedFunction<X> withBestTypes(TypedFunction<X> f) {
        return UnboxTransform.of(DhNullableTypeTransform.of(f));
    }

    private static class ParsedStates {
        private final Descriptor originalDescriptor;
        private final ProtobufOptions options;
        private final Map<Descriptor, ProtobufFunctions> parsed;

        private ParsedStates(Descriptor originalDescriptor, ProtobufOptions options) {
            this.originalDescriptor = Objects.requireNonNull(originalDescriptor);
            this.options = Objects.requireNonNull(options);
            this.parsed = new HashMap<>();
            getOrCreate(originalDescriptor);
        }

        public ProtobufFunctions functionsForSchemaChanges() {
            final Builder builder = ProtobufFunctions.builder();
            for (ProtobufFunction f : getOrCreate(originalDescriptor).functions()) {
                builder.addFunctions(ProtobufFunction.of(f.path(),
                        new ForPath(f.fieldNumberPath(), f.function().returnType()).adaptForSchemaChanges()));
            }
            return builder.build();
        }

        private ProtobufFunctions getOrCreate(Descriptor descriptor) {
            return parsed.computeIfAbsent(descriptor, this::create);
        }

        private ProtobufFunctions create(Descriptor newDescriptor) {
            if (!originalDescriptor.getFullName().equals(newDescriptor.getFullName())) {
                throw new IllegalArgumentException(String.format(
                        "Expected descriptor names to match. originalDescriptor.getFullName()=%s, newDescriptor.getFullName()=%s",
                        originalDescriptor.getFullName(), newDescriptor.getFullName()));
            }
            return withBestTypes(ProtobufFunctions.parse(newDescriptor, options));
        }

        private class ForPath {
            private final int[] originalFieldNumberPath;
            private final Type<?> originalType;
            private final Map<Descriptor, TypedFunction<Message>> functions;

            public ForPath(int[] originalFieldNumberPath, Type<?> originalType) {
                this.originalFieldNumberPath = Objects.requireNonNull(originalFieldNumberPath);
                this.originalType = Objects.requireNonNull(originalType);
                this.functions = new HashMap<>();
            }

            public TypedFunction<Message> adaptForSchemaChanges() {
                return originalType.walk(new AdaptForSchemaChanges());
            }

            private boolean applyAsBoolean(Message message) {
                return BooleanFunction.cast(getOrCreateForType(message)).applyAsBoolean(message);
            }

            private char applyAsChar(Message message) {
                return CharFunction.cast(getOrCreateForType(message)).applyAsChar(message);
            }

            private byte applyAsByte(Message message) {
                return ByteFunction.cast(getOrCreateForType(message)).applyAsByte(message);
            }

            private short applyAsShort(Message message) {
                return ShortFunction.cast(getOrCreateForType(message)).applyAsShort(message);
            }

            private int applyAsInt(Message message) {
                return IntFunction.cast(getOrCreateForType(message)).applyAsInt(message);
            }

            private long applyAsLong(Message message) {
                return LongFunction.cast(getOrCreateForType(message)).applyAsLong(message);
            }

            private float applyAsFloat(Message message) {
                return FloatFunction.cast(getOrCreateForType(message)).applyAsFloat(message);
            }

            private double applyAsDouble(Message message) {
                return DoubleFunction.cast(getOrCreateForType(message)).applyAsDouble(message);
            }

            private <T> T applyAsObject(Message message) {
                return ObjectFunction.<Message, T>cast(getOrCreateForType(message)).apply(message);
            }

            private TypedFunction<Message> getOrCreateForType(Message message) {
                return getOrCreate(message.getDescriptorForType());
            }

            private TypedFunction<Message> getOrCreate(Descriptor descriptor) {
                return functions.computeIfAbsent(descriptor, this::createFunctionFor);
            }

            private TypedFunction<Message> createFunctionFor(Descriptor descriptor) {
                final TypedFunction<Message> newFunction = ParsedStates.this.getOrCreate(descriptor)
                        .find(originalFieldNumberPath)
                        .map(ProtobufFunction::function)
                        .orElse(null);
                final TypedFunction<Message> adaptedFunction =
                        SchemaChangeAdaptFunction.of(newFunction, originalType).orElse(null);
                if (adaptedFunction == null) {
                    throw new UncheckedDeephavenException(
                            String.format("Incompatible schema change for %s, originalType=%s, newType=%s", "todo",
                                    originalType, newFunction == null ? null : newFunction.returnType()));
                }
                if (!originalType.equals(adaptedFunction.returnType())) {
                    // If this happens, must be a logical error in SchemaChangeAdaptFunction
                    throw new IllegalStateException(String.format(
                            "Expected adapted return types to be equal for %s, originalType=%s, adapatedType=%s",
                            "todo",
                            originalType, adaptedFunction.returnType()));
                }
                return adaptedFunction;
            }

            class AdaptForSchemaChanges
                    implements Visitor<TypedFunction<Message>>, PrimitiveType.Visitor<TypedFunction<Message>> {
                @Override
                public TypedFunction<Message> visit(PrimitiveType<?> primitiveType) {
                    return primitiveType.walk((PrimitiveType.Visitor<TypedFunction<Message>>) this);
                }

                @Override
                public ObjectFunction<Message, Object> visit(GenericType<?> genericType) {
                    // noinspection unchecked
                    return ObjectFunction.of(ForPath.this::applyAsObject, (GenericType<Object>) genericType);
                }

                @Override
                public BooleanFunction<Message> visit(BooleanType booleanType) {
                    return ForPath.this::applyAsBoolean;
                }

                @Override
                public ByteFunction<Message> visit(ByteType byteType) {
                    return ForPath.this::applyAsByte;
                }

                @Override
                public CharFunction<Message> visit(CharType charType) {
                    return ForPath.this::applyAsChar;
                }

                @Override
                public ShortFunction<Message> visit(ShortType shortType) {
                    return ForPath.this::applyAsShort;
                }

                @Override
                public IntFunction<Message> visit(IntType intType) {
                    return ForPath.this::applyAsInt;
                }

                @Override
                public LongFunction<Message> visit(LongType longType) {
                    return ForPath.this::applyAsLong;
                }

                @Override
                public FloatFunction<Message> visit(FloatType floatType) {
                    return ForPath.this::applyAsFloat;
                }

                @Override
                public DoubleFunction<Message> visit(DoubleType doubleType) {
                    return ForPath.this::applyAsDouble;
                }
            }
        }
    }

    private static class SchemaChangeAdaptFunction<T> implements TypedFunction.Visitor<T, TypedFunction<T>> {

        public static <T> Optional<TypedFunction<T>> of(TypedFunction<T> f, Type<?> desiredReturnType) {
            if (f == null) {
                return NullFunctions.of(desiredReturnType);
            }
            if (desiredReturnType.equals(f.returnType())) {
                return Optional.of(f);
            }
            return Optional.ofNullable(f.walk(new SchemaChangeAdaptFunction<>(desiredReturnType)));
        }

        private final Type<?> desiredReturnType;

        public SchemaChangeAdaptFunction(Type<?> desiredReturnType) {
            this.desiredReturnType = Objects.requireNonNull(desiredReturnType);
        }

        @Override
        public ObjectFunction<T, ?> visit(PrimitiveFunction<T> f) {
            if (desiredReturnType.equals(f.returnType().boxedType())) {
                return BoxTransform.of(f);
            }
            return null;
        }

        @Override
        public TypedFunction<T> visit(ObjectFunction<T, ?> f) {
            return f.returnType().walk(new GenericType.Visitor<>() {
                @Override
                public TypedFunction<T> visit(BoxedType<?> boxedType) {
                    if (desiredReturnType.equals(boxedType.primitiveType())) {
                        return boxedType.primitiveType().walk(new PrimitiveType.Visitor<>() {
                            @Override
                            public TypedFunction<T> visit(BooleanType booleanType) {
                                return null;
                            }

                            @Override
                            public TypedFunction<T> visit(ByteType byteType) {
                                return UnboxTransform.unboxByte(f.as(byteType.boxedType()));
                            }

                            @Override
                            public TypedFunction<T> visit(CharType charType) {
                                return UnboxTransform.unboxChar(f.as(charType.boxedType()));
                            }

                            @Override
                            public TypedFunction<T> visit(ShortType shortType) {
                                return UnboxTransform.unboxShort(f.as(shortType.boxedType()));
                            }

                            @Override
                            public TypedFunction<T> visit(IntType intType) {
                                return UnboxTransform.unboxInt(f.as(intType.boxedType()));
                            }

                            @Override
                            public TypedFunction<T> visit(LongType longType) {
                                return UnboxTransform.unboxLong(f.as(longType.boxedType()));
                            }

                            @Override
                            public TypedFunction<T> visit(FloatType floatType) {
                                return UnboxTransform.unboxFloat(f.as(floatType.boxedType()));
                            }

                            @Override
                            public TypedFunction<T> visit(DoubleType doubleType) {
                                return UnboxTransform.unboxDouble(f.as(doubleType.boxedType()));
                            }
                        });
                    }
                    return null;
                }

                @Override
                public TypedFunction<T> visit(StringType stringType) {
                    return null;
                }

                @Override
                public TypedFunction<T> visit(InstantType instantType) {
                    return null;
                }

                @Override
                public TypedFunction<T> visit(ArrayType<?, ?> arrayType) {
                    return null;
                }

                @Override
                public TypedFunction<T> visit(CustomType<?> customType) {
                    return null;
                }
            });
        }
    }
}
