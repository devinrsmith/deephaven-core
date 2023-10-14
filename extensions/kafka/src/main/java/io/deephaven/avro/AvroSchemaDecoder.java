package io.deephaven.avro;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.functions.ToBooleanFunction;
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.ToCharFunction;
import io.deephaven.functions.ToDoubleFunction;
import io.deephaven.functions.ToFloatFunction;
import io.deephaven.functions.ToIntFunction;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.ToPrimitiveFunction;
import io.deephaven.functions.ToShortFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType.Visitor;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.lang3.mutable.MutableInt;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

class AvroSchemaDecoder {
    private static final ToIntFunction<AvroParsingState> DECODE_INT = AvroParsingState::decodeInt;
    private static final ToLongFunction<AvroParsingState> DECODE_LONG = AvroParsingState::decodeLong;
    private static final ToFloatFunction<AvroParsingState> DECODE_FLOAT = AvroParsingState::decodeFloat;
    private static final ToDoubleFunction<AvroParsingState> DECODE_DOUBLE = AvroParsingState::decodeDouble;
    private static final ToBooleanFunction<AvroParsingState> DECODE_BOOLEAN = AvroParsingState::decodeBoolean;
    private static final ToObjectFunction<AvroParsingState, String> DECODE_STRING =
            ToObjectFunction.of(AvroParsingState::decodeString, io.deephaven.qst.type.Type.stringType());

    private static final ToIntFunction<AvroParsingState> DECODE_NULLABLE_INT_0 = AvroParsingState::decodeNullableInt0;
    private static final ToLongFunction<AvroParsingState> DECODE_NULLABLE_LONG_0 = AvroParsingState::decodeNullableLong0;
    private static final ToFloatFunction<AvroParsingState> DECODE_NULLABLE_FLOAT_0 =
            AvroParsingState::decodeNullableFloat0;
    private static final ToDoubleFunction<AvroParsingState> DECODE_NULLABLE_DOUBLE_0 =
            AvroParsingState::decodeNullableDouble0;
    private static final ToObjectFunction<AvroParsingState, String> DECODE_NULLABLE_STRING_0 =
            ToObjectFunction.of(AvroParsingState::decodeNullableString0, io.deephaven.qst.type.Type.stringType());
    private static final ToObjectFunction<AvroParsingState, Boolean> DECODE_NULLABLE_BOOLEAN_0 = ToObjectFunction
            .of(AvroParsingState::decodeNullableBoolean0, io.deephaven.qst.type.Type.booleanType().boxedType());
    private static final ToIntFunction<AvroParsingState> DECODE_NULLABLE_INT_1 = AvroParsingState::decodeNullableInt1;
    private static final ToLongFunction<AvroParsingState> DECODE_NULLABLE_LONG_1 = AvroParsingState::decodeNullableLong1;
    private static final ToFloatFunction<AvroParsingState> DECODE_NULLABLE_FLOAT_1 =
            AvroParsingState::decodeNullableFloat1;
    private static final ToDoubleFunction<AvroParsingState> DECODE_NULLABLE_DOUBLE_1 =
            AvroParsingState::decodeNullableDouble1;
    private static final ToObjectFunction<AvroParsingState, String> DECODE_NULLABLE_STRING_1 =
            ToObjectFunction.of(AvroParsingState::decodeNullableString1, io.deephaven.qst.type.Type.stringType());
    private static final ToObjectFunction<AvroParsingState, Boolean> DECODE_NULLABLE_BOOLEAN_1 = ToObjectFunction
            .of(AvroParsingState::decodeNullableBoolean1, io.deephaven.qst.type.Type.booleanType().boxedType());

    public static Writer of(Schema schema) {
        if (schema.getType() != Type.RECORD) {
            throw new IllegalArgumentException();
        }
        final List<ConsumerAndChunk> writers = new ArrayList<>();
        for (Field field : schema.getFields()) {
            final TypedFunction<AvroParsingState> f = function(field);
            final ColumnDefinition<?> definition = ColumnDefinition.of(field.name(), f.returnType());
            final BiConsumer<AvroParsingState, WritableChunk<?>> consumer = FunctionToChunkWriter.of(f);
            writers.add(new ConsumerAndChunk(definition, consumer));
        }
        return new Writer(writers);
    }

    public static class Writer {
        private final List<ConsumerAndChunk> writers;

        Writer(List<ConsumerAndChunk> writers) {
            this.writers = Objects.requireNonNull(writers);
        }

        public void writeAll(WritableChunk<?> [] chunks, byte[] bytes, int opos) {
            // I hate this chunk ownership model
            // This also assumes chunks already in correct order.
            if (chunks.length != writers.size()) {
                throw new IllegalArgumentException();
            }
            final MutableInt pos = new MutableInt(opos);
            final AvroParsingState state = new AvroParsingState(bytes, pos);
            final int L = writers.size();
            for (int i = 0; i < L; ++i) {
                writers.get(i).accept(state, chunks[i]);
            }
            if (pos.intValue() != bytes.length) {
                throw new IllegalStateException("Didn't consume all the bytes");
            }
        }
    }

    public static class ConsumerAndChunk {
        private final ColumnDefinition<?> definition;
        private final BiConsumer<AvroParsingState, WritableChunk<?>> consumer;

        ConsumerAndChunk(ColumnDefinition<?> definition, BiConsumer<AvroParsingState, WritableChunk<?>> consumer) {
            this.definition = Objects.requireNonNull(definition);
            this.consumer = Objects.requireNonNull(consumer);
        }

        public ColumnDefinition<?> columnDefinition() {
            return definition;
        }

        void accept(AvroParsingState state, WritableChunk<?> chunk) {
            consumer.accept(state, chunk);
        }
    }

    static TypedFunction<AvroParsingState> function(Field field) {
        final Schema schema = field.schema();
        switch (schema.getType()) {
            case UNION: {
                // assume a simple union for now
                final List<Schema> types = schema.getTypes();
                if (types.size() != 2) {
                    throw new RuntimeException();
                }
                final Type t0 = types.get(0).getType();
                final Type t1 = types.get(1).getType();
                if ((t0 == Type.NULL) == (t1 == Type.NULL)) {
                    throw new RuntimeException(String.format("Expected exactly one to be NULL, is %s %s", t0, t1));
                }
                if (t0 == Type.NULL) {
                    switch (t1) {
                        case STRING: {
                            return DECODE_NULLABLE_STRING_1;
                        }
                        case INT: {
                            return DECODE_NULLABLE_INT_1;
                        }
                        case LONG: {
                            return DECODE_NULLABLE_LONG_1;
                        }
                        case FLOAT: {
                            return DECODE_NULLABLE_FLOAT_1;
                        }
                        case DOUBLE: {
                            return DECODE_NULLABLE_DOUBLE_1;
                        }
                        case BOOLEAN: {
                            return DECODE_NULLABLE_BOOLEAN_1;
                        }
                        default:
                            throw new RuntimeException("Not impl");
                    }
                } else {
                    switch (t0) {
                        case STRING: {
                            return DECODE_NULLABLE_STRING_0;
                        }
                        case INT: {
                            return DECODE_NULLABLE_INT_0;
                        }
                        case LONG: {
                            return DECODE_NULLABLE_LONG_0;
                        }
                        case FLOAT: {
                            return DECODE_NULLABLE_FLOAT_0;
                        }
                        case DOUBLE: {
                            return DECODE_NULLABLE_DOUBLE_0;
                        }
                        case BOOLEAN: {
                            return DECODE_NULLABLE_BOOLEAN_0;
                        }
                        default:
                            throw new RuntimeException("Not impl");
                    }
                }
            }
            case STRING: {
                return DECODE_STRING;
            }
            case INT: {
                return DECODE_INT;
            }
            case LONG: {
                return DECODE_LONG;
            }
            case FLOAT: {
                return DECODE_FLOAT;
            }
            case DOUBLE: {
                return DECODE_DOUBLE;
            }
            case BOOLEAN: {
                return DECODE_BOOLEAN;
            }
            default: throw new RuntimeException("Not impl");
        }
    }

    // this code shouldn't be the responsibility of the parser
    private enum FunctionToChunkWriter
            implements TypedFunction.Visitor<AvroParsingState, BiConsumer<AvroParsingState, WritableChunk<?>>>,
            ToPrimitiveFunction.Visitor<AvroParsingState, BiConsumer<AvroParsingState, WritableChunk<?>>> {
        INSTANCE;

        public static BiConsumer<AvroParsingState, WritableChunk<?>> of(TypedFunction<AvroParsingState> f) {
            return f.walk(INSTANCE);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToPrimitiveFunction<AvroParsingState> f) {
            return f.walk(
                    (ToPrimitiveFunction.Visitor<AvroParsingState, BiConsumer<AvroParsingState, WritableChunk<?>>>) this);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToBooleanFunction<AvroParsingState> f) {
            return new BooleanToChunk(f);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToCharFunction<AvroParsingState> f) {
            return new CharToChunk(f);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToByteFunction<AvroParsingState> f) {
            return new ByteToChunk(f);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToShortFunction<AvroParsingState> f) {
            return new ShortToChunk(f);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToIntFunction<AvroParsingState> f) {
            return new IntToChunk(f);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToLongFunction<AvroParsingState> f) {
            return new LongToChunk(f);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToFloatFunction<AvroParsingState> f) {
            return new FloatToChunk(f);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToDoubleFunction<AvroParsingState> f) {
            return new DoubleToChunk(f);
        }

        @Override
        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ToObjectFunction<AvroParsingState, ?> f) {
            // todo: this is a sketch, incomplete
            final BiConsumer<AvroParsingState, WritableChunk<?>> consumer = f.returnType().walk(new Visitor<>() {
                @Override
                public BiConsumer<AvroParsingState, WritableChunk<?>> visit(BoxedType<?> boxedType) {
                    return boxedType.primitiveType().walk(new PrimitiveType.Visitor<>() {
                        @Override
                        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(BooleanType booleanType) {
                            return new BooleanToByteChunk(f.cast(booleanType.boxedType()));
                        }

                        @Override
                        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ByteType byteType) {
                            return null;
                        }

                        @Override
                        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(CharType charType) {
                            return null;
                        }

                        @Override
                        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ShortType shortType) {
                            return null;
                        }

                        @Override
                        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(IntType intType) {
                            return null;
                        }

                        @Override
                        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(LongType longType) {
                            return null;
                        }

                        @Override
                        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(FloatType floatType) {
                            return null;
                        }

                        @Override
                        public BiConsumer<AvroParsingState, WritableChunk<?>> visit(DoubleType doubleType) {
                            return null;
                        }
                    });
                }

                @Override
                public BiConsumer<AvroParsingState, WritableChunk<?>> visit(StringType stringType) {
                    return null;
                }

                @Override
                public BiConsumer<AvroParsingState, WritableChunk<?>> visit(InstantType instantType) {
                    return new InstantToLongChunk(f.cast(instantType));
                }

                @Override
                public BiConsumer<AvroParsingState, WritableChunk<?>> visit(ArrayType<?, ?> arrayType) {
                    return null;
                }

                @Override
                public BiConsumer<AvroParsingState, WritableChunk<?>> visit(CustomType<?> customType) {
                    return null;
                }
            });
            if (consumer != null) {
                return consumer;
            }
            // noinspection unchecked
            return new ObjectToChunk((ToObjectFunction<AvroParsingState, Object>) f);
        }
    }

    private static class BooleanToChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToBooleanFunction<AvroParsingState> f;

        public BooleanToChunk(ToBooleanFunction<AvroParsingState> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableBooleanChunk().add(f.test(state));
        }
    }

    private static class CharToChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToCharFunction<AvroParsingState> f;

        public CharToChunk(ToCharFunction<AvroParsingState> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableCharChunk().add(f.applyAsChar(state));
        }
    }

    private static class ByteToChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToByteFunction<AvroParsingState> f;

        public ByteToChunk(ToByteFunction<AvroParsingState> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableByteChunk().add(f.applyAsByte(state));
        }
    }

    private static class ShortToChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToShortFunction<AvroParsingState> f;

        public ShortToChunk(ToShortFunction<AvroParsingState> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableShortChunk().add(f.applyAsShort(state));
        }
    }

    private static class IntToChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToIntFunction<AvroParsingState> f;

        public IntToChunk(ToIntFunction<AvroParsingState> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableIntChunk().add(f.applyAsInt(state));
        }
    }

    private static class LongToChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToLongFunction<AvroParsingState> f;

        public LongToChunk(ToLongFunction<AvroParsingState> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableLongChunk().add(f.applyAsLong(state));
        }
    }

    private static class FloatToChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToFloatFunction<AvroParsingState> f;

        public FloatToChunk(ToFloatFunction<AvroParsingState> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableFloatChunk().add(f.applyAsFloat(state));
        }
    }

    private static class DoubleToChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToDoubleFunction<AvroParsingState> f;

        public DoubleToChunk(ToDoubleFunction<AvroParsingState> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableDoubleChunk().add(f.applyAsDouble(state));
        }
    }

    private static class BooleanToByteChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToObjectFunction<AvroParsingState, Boolean> f;

        public BooleanToByteChunk(ToObjectFunction<AvroParsingState, Boolean> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableByteChunk().add(BooleanUtils.booleanAsByte(f.apply(state)));
        }
    }

    private static class InstantToLongChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToObjectFunction<AvroParsingState, Instant> f;

        public InstantToLongChunk(ToObjectFunction<AvroParsingState, Instant> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableLongChunk().add(DateTimeUtils.epochNanos(f.apply(state)));
        }
    }

    private static class ObjectToChunk implements BiConsumer<AvroParsingState, WritableChunk<?>> {
        private final ToObjectFunction<AvroParsingState, Object> f;

        public ObjectToChunk(ToObjectFunction<AvroParsingState, Object> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void accept(AvroParsingState state, WritableChunk<?> chunk) {
            chunk.asWritableObjectChunk().add(f.apply(state));
        }
    }
}
