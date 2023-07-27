package io.deephaven.protobuf;

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FieldMask;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

class Builtin {
    static List<SingleValuedMessageParser> parsers() {
        return List.of(
                TimestampParser.of(),
                DurationParser.of(),
                BoolValueParser.of(),
                Int32ValueParser.of(),
                UInt32ValueParser.of(),
                Int64ValueParser.of(),
                UInt64ValueParser.of(),
                FloatValueParser.of(),
                DoubleValueParser.of(),
                StringValueParser.of(),
                BytesValueParser.of(),
                customParser(Any.class),
                customParser(FieldMask.class));
    }

    private static <R extends Message> ObjectFunction<Message, R> message(GenericType<R> type) {
        return ObjectFunction.cast(type);
    }

    static <T extends Message> SingleValuedMessageParser customParser(Class<T> clazz) {
        try {
            final Method method = clazz.getDeclaredMethod("getDescriptor");
            final Descriptor descriptor = (Descriptor) method.invoke(null);
            return new GenericSVMP<>(CustomType.of(clazz), descriptor);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private enum TimestampParser implements SingleValuedMessageParser, Function<Timestamp, Instant> {
        INSTANCE;

        private static final CustomType<Timestamp> IN_TYPE = Type.ofCustom(Timestamp.class);
        private static final InstantType OUT_TYPE = Type.instantType();

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return Timestamp.getDescriptor();
        }

        @Override
        public ObjectFunction<Message, Instant> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapObj(this, OUT_TYPE);
        }

        @Override
        public Instant apply(Timestamp t) {
            return Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
        }
    }

    private enum DurationParser implements SingleValuedMessageParser, Function<com.google.protobuf.Duration, Duration> {
        INSTANCE;

        private static final CustomType<com.google.protobuf.Duration> IN_TYPE =
                Type.ofCustom(com.google.protobuf.Duration.class);
        private static final CustomType<Duration> OUT_TYPE = Type.ofCustom(Duration.class);

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return com.google.protobuf.Duration.getDescriptor();
        }

        @Override
        public ObjectFunction<Message, Duration> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapObj(this, OUT_TYPE);
        }

        @Override
        public Duration apply(com.google.protobuf.Duration d) {
            return Duration.ofSeconds(d.getSeconds(), d.getNanos());
        }
    }

    private enum BoolValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<BoolValue> IN_TYPE = Type.ofCustom(BoolValue.class);

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return BoolValue.getDescriptor();
        }

        @Override
        public BooleanFunction<Message> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapBoolean(BoolValue::getValue);
        }
    }

    private enum Int32ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<Int32Value> IN_TYPE = Type.ofCustom(Int32Value.class);

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return Int32Value.getDescriptor();
        }

        @Override
        public IntFunction<Message> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapInt(Int32Value::getValue);
        }
    }

    private enum UInt32ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<UInt32Value> IN_TYPE = Type.ofCustom(UInt32Value.class);

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return UInt32Value.getDescriptor();
        }

        @Override
        public IntFunction<Message> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapInt(UInt32Value::getValue);
        }
    }

    private enum Int64ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<Int64Value> IN_TYPE = Type.ofCustom(Int64Value.class);

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return Int64Value.getDescriptor();
        }

        @Override
        public LongFunction<Message> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapLong(Int64Value::getValue);
        }
    }

    private enum UInt64ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<UInt64Value> IN_TYPE = Type.ofCustom(UInt64Value.class);

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return UInt64Value.getDescriptor();
        }

        @Override
        public LongFunction<Message> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapLong(UInt64Value::getValue);
        }
    }

    private enum FloatValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<FloatValue> IN_TYPE = Type.ofCustom(FloatValue.class);

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return FloatValue.getDescriptor();
        }

        @Override
        public FloatFunction<Message> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapFloat(FloatValue::getValue);
        }
    }

    private enum DoubleValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<DoubleValue> IN_TYPE = Type.ofCustom(DoubleValue.class);

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return DoubleValue.getDescriptor();
        }

        @Override
        public DoubleFunction<Message> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapDouble(DoubleValue::getValue);
        }
    }

    private enum StringValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<StringValue> IN_TYPE = Type.ofCustom(StringValue.class);
        private static final StringType OUT_TYPE = Type.stringType();

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return StringValue.getDescriptor();
        }

        @Override
        public ObjectFunction<Message, String> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapObj(StringValue::getValue, OUT_TYPE);
        }
    }

    private enum BytesValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<BytesValue> IN_TYPE = Type.ofCustom(BytesValue.class);
        private static final NativeArrayType<byte[], Byte> OUT_TYPE = Type.byteType().arrayType();

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor descriptor() {
            return BytesValue.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(ProtobufOptions options) {
            return message(IN_TYPE).mapObj(BytesValueParser::toBytes, OUT_TYPE);
        }

        private static byte[] toBytes(BytesValue bv) {
            return bv.getValue().toByteArray();
        }
    }

    private static class GenericSVMP<T extends Message> implements SingleValuedMessageParser {
        private final GenericType<T> type;
        private final Descriptor descriptor;

        public GenericSVMP(GenericType<T> type, Descriptor descriptor) {
            this.type = Objects.requireNonNull(type);
            this.descriptor = Objects.requireNonNull(descriptor);
        }

        @Override
        public Descriptor descriptor() {
            return descriptor;
        }

        @Override
        public TypedFunction<Message> messageParser(ProtobufOptions options) {
            return message(type);
        }
    }
}
