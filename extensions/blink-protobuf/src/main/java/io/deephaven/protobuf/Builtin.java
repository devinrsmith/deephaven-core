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
                TimestampParser.INSTANCE,
                DurationParser.INSTANCE,
                BoolValueParser.INSTANCE,
                Int32ValueParser.INSTANCE,
                UInt32ValueParser.INSTANCE,
                Int64ValueParser.INSTANCE,
                UInt64ValueParser.INSTANCE,
                FloatValueParser.INSTANCE,
                DoubleValueParser.INSTANCE,
                StringValueParser.INSTANCE,
                BytesValueParser.INSTANCE,
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

        @Override
        public String fullName() {
            return Timestamp.getDescriptor().getFullName();
        }

        @Override
        public ObjectFunction<Message, Instant> parser(ProtobufOptions options) {
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

        @Override
        public String fullName() {
            return com.google.protobuf.Duration.getDescriptor().getFullName();
        }

        @Override
        public ObjectFunction<Message, Duration> parser(ProtobufOptions options) {
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

        @Override
        public String fullName() {
            return BoolValue.getDescriptor().getFullName();
        }

        @Override
        public BooleanFunction<Message> parser(ProtobufOptions options) {
            return message(IN_TYPE).mapBoolean(BoolValue::getValue);
        }
    }

    private enum Int32ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<Int32Value> IN_TYPE = Type.ofCustom(Int32Value.class);

        @Override
        public String fullName() {
            return Int32Value.getDescriptor().getFullName();
        }

        @Override
        public IntFunction<Message> parser(ProtobufOptions options) {
            return message(IN_TYPE).mapInt(Int32Value::getValue);
        }
    }

    private enum UInt32ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<UInt32Value> IN_TYPE = Type.ofCustom(UInt32Value.class);


        @Override
        public String fullName() {
            return UInt32Value.getDescriptor().getFullName();
        }

        @Override
        public IntFunction<Message> parser(ProtobufOptions options) {
            return message(IN_TYPE).mapInt(UInt32Value::getValue);
        }
    }

    private enum Int64ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<Int64Value> IN_TYPE = Type.ofCustom(Int64Value.class);


        @Override
        public String fullName() {
            return Int64Value.getDescriptor().getFullName();
        }

        @Override
        public LongFunction<Message> parser(ProtobufOptions options) {
            return message(IN_TYPE).mapLong(Int64Value::getValue);
        }
    }

    private enum UInt64ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<UInt64Value> IN_TYPE = Type.ofCustom(UInt64Value.class);


        @Override
        public String fullName() {
            return UInt64Value.getDescriptor().getFullName();
        }

        @Override
        public LongFunction<Message> parser(ProtobufOptions options) {
            return message(IN_TYPE).mapLong(UInt64Value::getValue);
        }
    }

    private enum FloatValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<FloatValue> IN_TYPE = Type.ofCustom(FloatValue.class);

        @Override
        public String fullName() {
            return FloatValue.getDescriptor().getFullName();
        }

        @Override
        public FloatFunction<Message> parser(ProtobufOptions options) {
            return message(IN_TYPE).mapFloat(FloatValue::getValue);
        }
    }

    private enum DoubleValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<DoubleValue> IN_TYPE = Type.ofCustom(DoubleValue.class);

        @Override
        public String fullName() {
            return DoubleValue.getDescriptor().getFullName();
        }

        @Override
        public DoubleFunction<Message> parser(ProtobufOptions options) {
            return message(IN_TYPE).mapDouble(DoubleValue::getValue);
        }
    }

    private enum StringValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<StringValue> IN_TYPE = Type.ofCustom(StringValue.class);
        private static final StringType OUT_TYPE = Type.stringType();

        @Override
        public String fullName() {
            return StringValue.getDescriptor().getFullName();
        }

        @Override
        public ObjectFunction<Message, String> parser(ProtobufOptions options) {
            return message(IN_TYPE).mapObj(StringValue::getValue, OUT_TYPE);
        }
    }

    private enum BytesValueParser implements SingleValuedMessageParser {
        INSTANCE;

        private static final CustomType<BytesValue> IN_TYPE = Type.ofCustom(BytesValue.class);
        private static final NativeArrayType<byte[], Byte> OUT_TYPE = Type.byteType().arrayType();

        @Override
        public String fullName() {
            return BytesValue.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parser(ProtobufOptions options) {
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
        public String fullName() {
            return descriptor.getFullName();
        }

        @Override
        public TypedFunction<Message> parser(ProtobufOptions options) {
            return message(type);
        }
    }
}
