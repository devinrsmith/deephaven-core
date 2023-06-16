package io.deephaven.protobuf;

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FieldMask;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.UnknownFieldSet;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.ApplyVisitor;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Protobuf {

    private static final CustomType<EnumValueDescriptor> ENUM_TYPE = Type.ofCustom(EnumValueDescriptor.class);
    private static final CustomType<Message> MESSAGE_TYPE = Type.ofCustom(Message.class);
    private static final CustomType<ByteString> BYTE_STRING_TYPE = Type.ofCustom(ByteString.class);
    public static final CustomType<UnknownFieldSet> UNKNOWN_FIELD_SET_TYPE = Type.ofCustom(UnknownFieldSet.class);
    public static final CustomType<Unmapped> UNMAPPED_TYPE = Type.ofCustom(Unmapped.class);

    public interface Unmapped {
    }

    public static GenericMessageParser parser(Descriptor descriptor) {
        return parserImpl(descriptor, List.of());
    }

    private static GenericMessageParser parserImpl(Descriptor descriptor, List<String> context) {
        final List<String> copyContext = List.copyOf(context);
        return new GenericMessageParser() {
            @Override
            public String fullName() {
                return descriptor.getFullName();
            }

            @Override
            public Map<List<String>, TypedFunction<Message>> parser(ProtobufOptions options) {
                return messageToValues(descriptor, options, copyContext);
            }
        };
    }

    private static Map<List<String>, TypedFunction<Message>> messageToValues(
            Descriptor descriptor,
            ProtobufOptions options,
            List<String> context) {
        if (!options.include(context)) {
            return Map.of();
        }
        final Map<List<String>, TypedFunction<Message>> map = new LinkedHashMap<>();
        final SingleValuedMessageParser svmp = options.parsers().get(descriptor.getFullName());
        if (svmp != null) {
            map.put(context, svmp.parser(options));
        } else {
            for (FieldDescriptor field : descriptor.getFields()) {
                map.putAll(messageFieldToValues(field, options, context));
            }
        }
        options.serializedSizeName(descriptor, options, context).ifPresent(name -> map.put(path(context, name), serializedSizeFunction()));
        options.unknownFieldSetName(descriptor, options, context).ifPresent(name -> map.put(path(context, name), unknownFieldsFunction()));
        options.rawMessageName(descriptor, options, context).ifPresent(name -> map.put(path(context, name), rawMessageFunction()));
        return map;
    }

    private static Map<List<String>, TypedFunction<Message>> messageFieldToValues(
            FieldDescriptor fd, ProtobufOptions options, List<String> context) {
        final String name = fd.getName();
        final List<String> fieldPath = path(context, name);
        if (!options.include(fieldPath)) {
            return Map.of();
        }

        final TypedFunction<Message> tf = messageFieldToValue(fd, options);
        if (tf != null) {
            return Map.of(fieldPath, tf);
        }

        // All simple and known messages types should already be handled

        if (fd.getJavaType() != JavaType.MESSAGE) {
            throw new IllegalStateException();
        }

        if (!fd.isRepeated()) {
            return options.parseAdHocMessage(fd, options, context)
                    ? singularMessageFunctions(fd, options, fieldPath)
                    : Map.of(fieldPath, messageFieldFunction(fd));
        } else {
            return options.parseAdHocRepeatedMessage(fd, options, context)
                    ? repeatedMessageFunctions(fd, options, fieldPath)
                    : Map.of(fieldPath, repeatedGenericFunction(fd, MESSAGE_TYPE.arrayType()));
        }
    }

    private static Map<List<String>, TypedFunction<Message>> singularMessageFunctions(FieldDescriptor fd, ProtobufOptions options, List<String> fieldPath) {
        final Descriptor messageType = fd.getMessageType();
        final Function<Message, Message> messageFunction = messageFieldFunction(fd)::apply;
        final Map<List<String>, TypedFunction<Message>> nf = messageToValues(messageType, options, fieldPath);
        final Map<List<String>, TypedFunction<Message>> output = new LinkedHashMap<>(nf.size());
        for (Entry<List<String>, TypedFunction<Message>> e : nf.entrySet()) {
            final TypedFunction<Message> innerFunction = e.getValue();
            final TypedFunction<Message> adaptedFunction = innerFunction.mapInput(messageFunction);
            output.put(e.getKey(), adaptedFunction);
        }
        return output;
    }

    private static Map<List<String>, TypedFunction<Message>> repeatedMessageFunctions(FieldDescriptor fd, ProtobufOptions options, List<String> fieldPath) {
        throw new UnsupportedOperationException();
//        final Descriptor messageType = fd.getMessageType();
////        final Function<Message, Message> messageFunction = messageFunction(fd)::apply;
//        final Map<List<String>, TypedFunction<Message>> nf = namedFunctions(messageType, options, fieldPath);
//        final Map<List<String>, TypedFunction<Message>> output = new LinkedHashMap<>(nf.size());
//        for (Entry<List<String>, TypedFunction<Message>> e : nf.entrySet()) {
//            final TypedFunction<Message> innerFunction = e.getValue();
//            final TypedFunction<Message> adaptedFunction = innerFunction.mapInput(messageFunction);
//            output.put(e.getKey(), adaptedFunction);
//        }
//        return output;
    }

    private static List<String> path(List<String> context, String name) {
        return Stream.concat(context.stream(), Stream.of(name)).collect(Collectors.toList());
    }

    private static TypedFunction<Message> messageFieldToValue(FieldDescriptor fd, ProtobufOptions options) {
        if (fd.isMapField()) {
            // special case? todo, make optional
            return mapFunction(fd, options);
        }
        return !fd.isRepeated()
                ? singleMessageFieldToValue(fd, options)
                : repeatedMessageFieldToValue(fd, options);
    }

    private static TypedFunction<Message> singleMessageFieldToValue(FieldDescriptor fd, ProtobufOptions options) {
        return messageFieldToValue(fieldFunction(fd), fd, options);
    }

    private static ObjectFunction<Message, ?> repeatedMessageFieldToValue(FieldDescriptor fd, ProtobufOptions options) {
        final int[] index = new int[] { 0 };
        final TypedFunction<Message> tf = messageFieldToValue(ObjectFunction.of(m -> m.getRepeatedField(fd, index[0]), CustomType.of(Object.class)), fd, options);
        if (tf == null) {
            return null;
        }
        return tf.walk(new TypedFunction.Visitor<>() {
            @Override
            public ObjectFunction<Message, ?> visit(BooleanFunction<Message> f) {
                // todo
                return repeatedGenericFunction(fd, Boolean.class, b -> null, UNMAPPED_TYPE.arrayType());
            }

            @Override
            public ObjectFunction<Message, char[]> visit(CharFunction<Message> f) {
                return ObjectFunction.of(message -> {
                    final int count = message.getRepeatedFieldCount(fd);
                    final char[] out = new char[count];
                    for (int i = 0; i < count; ++i) {
                        index[0] = i;
                        out[i] = f.applyAsChar(message);
                    }
                    return out;
                }, Type.charType().arrayType());
            }

            @Override
            public ObjectFunction<Message, byte[]> visit(ByteFunction<Message> f) {
                return ObjectFunction.of(message -> {
                    final int count = message.getRepeatedFieldCount(fd);
                    final byte[] out = new byte[count];
                    for (int i = 0; i < count; ++i) {
                        index[0] = i;
                        out[i] = f.applyAsByte(message);
                    }
                    return out;
                }, Type.byteType().arrayType());
            }

            @Override
            public ObjectFunction<Message, short[]> visit(ShortFunction<Message> f) {
                return ObjectFunction.of(message -> {
                    final int count = message.getRepeatedFieldCount(fd);
                    final short[] out = new short[count];
                    for (int i = 0; i < count; ++i) {
                        index[0] = i;
                        out[i] = f.applyAsShort(message);
                    }
                    return out;
                }, Type.shortType().arrayType());
            }

            @Override
            public ObjectFunction<Message, int[]> visit(IntFunction<Message> f) {
                return ObjectFunction.of(message -> {
                    final int count = message.getRepeatedFieldCount(fd);
                    final int[] out = new int[count];
                    for (int i = 0; i < count; ++i) {
                        index[0] = i;
                        out[i] = f.applyAsInt(message);
                    }
                    return out;
                }, Type.intType().arrayType());
            }

            @Override
            public ObjectFunction<Message, long[]> visit(LongFunction<Message> f) {
                return ObjectFunction.of(message -> {
                    final int count = message.getRepeatedFieldCount(fd);
                    final long[] out = new long[count];
                    for (int i = 0; i < count; ++i) {
                        index[0] = i;
                        out[i] = f.applyAsLong(message);
                    }
                    return out;
                }, Type.longType().arrayType());
            }

            @Override
            public ObjectFunction<Message, float[]> visit(FloatFunction<Message> f) {
                return ObjectFunction.of(message -> {
                    final int count = message.getRepeatedFieldCount(fd);
                    final float[] out = new float[count];
                    for (int i = 0; i < count; ++i) {
                        index[0] = i;
                        out[i] = f.applyAsFloat(message);
                    }
                    return out;
                }, Type.floatType().arrayType());
            }

            @Override
            public ObjectFunction<Message, double[]> visit(DoubleFunction<Message> f) {
                return ObjectFunction.of(message -> {
                    final int count = message.getRepeatedFieldCount(fd);
                    final double[] out = new double[count];
                    for (int i = 0; i < count; ++i) {
                        index[0] = i;
                        out[i] = f.applyAsDouble(message);
                    }
                    return out;
                }, Type.doubleType().arrayType());
            }

            @Override
            public ObjectFunction<Message, ?> visit(ObjectFunction<Message, ?> f) {
                return visitImpl(f);
            }

            private <R> ObjectFunction<Message, R[]> visitImpl(ObjectFunction<Message, R> f) {
                final GenericType<R> rt = f.returnType();
                final NativeArrayType<R[], R> arrayType = rt.arrayType();
                return ObjectFunction.of(message -> {
                    final int count = message.getRepeatedFieldCount(fd);
                    final R[] out = arrayType.newArrayInstance(count);
                    for (int i = 0; i < count; ++i) {
                        index[0] = i;
                        out[i] = f.apply(message);
                    }
                    return out;
                }, arrayType);
            }
        });
    }

    private static TypedFunction<Message> messageFieldToValue(ObjectFunction<Message, Object> messageToObject, FieldDescriptor fd, ProtobufOptions options) {
        switch (fd.getJavaType()) {
            case BOOLEAN:
                return messageToObject.map(NullGuard.of(booleanLiteral()));
            case INT:
                return messageToObject.map(NullGuard.of(intLiteral()));
            case LONG:
                return messageToObject.map(NullGuard.of(longLiteral()));
            case FLOAT:
                return messageToObject.map(NullGuard.of(floatLiteral()));
            case DOUBLE:
                return messageToObject.map(NullGuard.of(doubleLiteral()));
            case STRING:
                return messageToObject.as(Type.stringType());
            case ENUM:
                return messageToObject.as(ENUM_TYPE);
            case BYTE_STRING:
                return messageToObject.as(BYTE_STRING_TYPE).map(NullGuard.of(ObjectFunction.of(ByteString::toByteArray, Type.byteType().arrayType())));
            case MESSAGE:
                final SingleValuedMessageParser parser = options.parsers().get(fd.getMessageType().getFullName());
                if (parser == null) {
                    // todo: consider recusing here, change return type for multi-value?
                    return null;
                }
                return messageToObject.as(MESSAGE_TYPE).map(NullGuard.of(parser.parser(options)));
            default:
                throw new IllegalStateException();
        }
    }

    private static ObjectFunction<Message, UnknownFieldSet> unknownFieldsFunction() {
        return ObjectFunction.of(MessageOrBuilder::getUnknownFields, UNKNOWN_FIELD_SET_TYPE);
    }

    private static IntFunction<Message> serializedSizeFunction() {
        return Message::getSerializedSize;
    }

    private static ObjectFunction<Message, Message> rawMessageFunction() {
        return ObjectFunction.of(Function.identity(), MESSAGE_TYPE);
    }

    private static ObjectFunction<Message, ?> mapFunction(FieldDescriptor fd, ProtobufOptions options) {
        final FieldDescriptor keyFd = fd.getMessageType().findFieldByNumber(1);
        final TypedFunction<Message> keyTf = messageFieldToValue(keyFd, options);
        if (keyTf == null) {
            return null;
        }
        final FieldDescriptor valueFd = fd.getMessageType().findFieldByNumber(2);
        final TypedFunction<Message> valueTf = messageFieldToValue(valueFd, options);
        if (valueTf == null) {
            return null;
        }
        // todo: proper Map types?
        return ObjectFunction.of(message -> {
            final Map<Object, Object> map = new LinkedHashMap<>();
            final int count = message.getRepeatedFieldCount(fd);
            for (int i = 0; i < count; ++i) {
                final Message entry = (Message) message.getRepeatedField(fd, i);
                // This is inefficient, but we are currently using Map; so, may not matter.
                final Object key = keyTf.walk(new ApplyVisitor<>(entry));
                final Object value = valueTf.walk(new ApplyVisitor<>(entry));
                map.put(key, value);
            }
            return map;
        }, Type.ofCustom(Map.class));
    }

    // ----------------------------------------------------------------------------------------------------------------

    // Note: in protobuf an actualized Message is never null.
    // In the case of our translation layer though, the presence of a null Message means that the field, or some parent
    // of the field, was not present; but we still need to translate the value for the specific column.
    private static boolean hasField(Message m, FieldDescriptor fd) {
        return m != null && (m.hasField(fd) || !fd.hasPresence());
    }

    // All our calls to Message#getField are guarded by #hasField.



//    private static <T> Optional<T> objectApply(Message m, FieldDescriptor fd, ObjectFunction<Object, T> toObject) {
//        return hasField(m, fd)
//                ? Optional.of(toObject.apply(m.getField(fd)))
//                : Optional.empty();
//    }

    // ----------------------------------------------------------------------------------------------------------------

//    private static <T, M extends Message> ObjectFunction<M, T> castFunction(FieldDescriptor fd, GenericType<T> type) {
//        return ObjectFunction.of(m -> objectApply(m, fd, castTo(type)).orElse(null), type);
//    }


    private static <ArrayType, ComponentType> ObjectFunction<Message, ArrayType> repeatedGenericFunction(
            FieldDescriptor fd,
            NativeArrayType<ArrayType, ComponentType> returnType) {
        return repeatedGenericFunction(fd, returnType.componentType().clazz(), Function.identity(), returnType);
    }

    private static <T, ArrayType, ComponentType> ObjectFunction<Message, ArrayType> repeatedGenericFunction(
            FieldDescriptor fd,
            Class<T> intermediateType,
            Function<T, ComponentType> adapter,
            NativeArrayType<ArrayType, ComponentType> returnType) {
        return ObjectFunction.of(m -> repeatedGenericF(m, fd, intermediateType, adapter, returnType), returnType);
    }

    private static <T, ArrayType, ComponentType> ArrayType repeatedGenericF(
            Message m,
            FieldDescriptor fd,
            Class<T> intermediateType,
            Function<T, ComponentType> adapter,
            NativeArrayType<ArrayType, ComponentType> returnType) {
        final int count = m.getRepeatedFieldCount(fd);
        final ArrayType array = returnType.newArrayInstance(count);
        for (int i = 0; i < count; ++i) {
            final ComponentType value = adapter.apply(repeatedObjectValue(m, fd, i, intermediateType));
            returnType.set(array, i, value);
        }
        return array;
    }



    private static <T> T repeatedObjectValue(Message m, FieldDescriptor fd, int index, Class<T> clazz) {
        return clazz.cast(m.getRepeatedField(fd, index));
    }


    private static BooleanFunction<Object> booleanLiteral() {
        return o -> (boolean) o;
    }

    private static IntFunction<Object> intLiteral() {
        return o -> (int) o;
    }

    private static LongFunction<Object> longLiteral() {
        return o -> (long) o;
    }

    private static FloatFunction<Object> floatLiteral() {
        return o -> (float) o;
    }

    private static DoubleFunction<Object> doubleLiteral() {
        return o -> (double) o;
    }

    private static <T> ObjectFunction<Object, T> castTo(GenericType<T> type) {
        return ObjectFunction.of(o -> type.clazz().cast(o), type);
    }

    static List<SingleValuedMessageParser> builtinParsers() {
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

    private static ObjectFunction<Message, Object> fieldFunction(FieldDescriptor fd) {
        return ObjectFunction.of(message -> hasField(message, fd) ? message.getField(fd) : null, Type.ofCustom(Object.class));
    }

    private static ObjectFunction<Message, Message> messageFieldFunction(FieldDescriptor fd) {
        return fieldFunction(fd).as(MESSAGE_TYPE);
    }

    private static <R extends Message> ObjectFunction<Message, R> message(GenericType<R> type) {
        return ObjectFunction.cast(type);
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
            return message(IN_TYPE).map(this, OUT_TYPE);
        }

        @Override
        public Instant apply(Timestamp t) {
            return Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
        }
    }

    private enum DurationParser implements SingleValuedMessageParser, Function<com.google.protobuf.Duration, Duration> {
        INSTANCE;

        private static final CustomType<com.google.protobuf.Duration> IN_TYPE = Type.ofCustom(com.google.protobuf.Duration.class);
        private static final CustomType<Duration> OUT_TYPE = Type.ofCustom(Duration.class);

        @Override
        public String fullName() {
            return com.google.protobuf.Duration.getDescriptor().getFullName();
        }

        @Override
        public ObjectFunction<Message, Duration> parser(ProtobufOptions options) {
            return message(IN_TYPE).map(this, OUT_TYPE);
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
            return message(IN_TYPE).map(BoolValue::getValue);
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
            return message(IN_TYPE).map(Int32Value::getValue);
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
            return message(IN_TYPE).map(UInt32Value::getValue);
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
            return message(IN_TYPE).map(Int64Value::getValue);
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
            return message(IN_TYPE).map(UInt64Value::getValue);
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
            return message(IN_TYPE).map(FloatValue::getValue);
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
            return message(IN_TYPE).map(DoubleValue::getValue);
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
            return message(IN_TYPE).map(StringValue::getValue, OUT_TYPE);
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
            return message(IN_TYPE).map(BytesValueParser::toBytes, OUT_TYPE);
        }

        private static byte[] toBytes(BytesValue bv) {
            return bv.getValue().toByteArray();
        }
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
