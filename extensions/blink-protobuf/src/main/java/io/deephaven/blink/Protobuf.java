package io.deephaven.blink;

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
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.util.QueryConstants;
import io.deephaven.vector.BooleanVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
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

    static Map<List<String>, TypedFunction<Message>> namedFunctions(Descriptor descriptor, ProtobufOptions options) {
        return namedFunctions(descriptor, options, List.of());
    }

    private static Map<List<String>, TypedFunction<Message>> namedFunctions(Descriptor descriptor,
            ProtobufOptions options, List<String> context) {
        final Map<List<String>, TypedFunction<Message>> map = new LinkedHashMap<>();
        for (FieldDescriptor field : descriptor.getFields()) {
            map.putAll(namedFunctions(field, options, context));
        }
        options.serializedSizeName().ifPresent(name -> map.put(List.of(name), serializedSizeFunction()));
        options.unknownFieldSetName().ifPresent(name -> map.put(List.of(name), unknownFieldsFunction()));
        options.rawMessageName().ifPresent(name -> map.put(List.of(name), rawMessageFunction()));
        return map;
    }

    private static Map<List<String>, TypedFunction<Message>> namedFunctions(FieldDescriptor fd, ProtobufOptions options,
            List<String> context) {
        final String name = fd.getName();
        final List<String> fieldPath = path(context, name);
        if (!options.include(fieldPath)) {
            return Map.of();
        }

        final TypedFunction<Message> tf = typedFunction(fd, options);
        if (tf != null) {
            return Map.of(fieldPath, tf);
        }

        if (fd.getJavaType() != JavaType.MESSAGE) {
            throw new IllegalStateException();
        }

        if (!fd.isRepeated() && options.parseAdHocMessage()) {
            final Function<Message, Message> messageFunction = messageFunction(fd)::apply;
            final Descriptor messageType = fd.getMessageType();
            final Map<List<String>, TypedFunction<Message>> nf = namedFunctions(messageType, options, fieldPath);
            final Map<List<String>, TypedFunction<Message>> output = new LinkedHashMap<>(nf.size());
            for (Entry<List<String>, TypedFunction<Message>> e : nf.entrySet()) {
                final TypedFunction<Message> innerFunction = e.getValue();
                final TypedFunction<Message> adaptedFunction = innerFunction.mapInput(messageFunction);
                output.put(e.getKey(), adaptedFunction);
            }
            return output;
        }

        // Fallback to simple Message.class type
        return Map.of(fieldPath, fd.isRepeated()
                ? repeatedGenericFunction(fd, MESSAGE_TYPE.arrayType())
                : castFunction(fd, MESSAGE_TYPE));
    }

    private static List<String> path(List<String> context, String name) {
        return Stream.concat(context.stream(), Stream.of(name)).collect(Collectors.toList());
    }

    private static TypedFunction<Message> typedFunction(FieldDescriptor fd, ProtobufOptions options) {
        if (fd.isMapField()) {
            return mapFunction(fd, options);
        }
        if (fd.isRepeated()) {
            return repeatedTypedFunction(fd, options);
        }
        switch (fd.getJavaType()) {
            case BOOLEAN:
                return booleanFunction(fd, booleanLiteral());
            case INT:
                return intFunction(fd, intLiteral());
            case LONG:
                return longFunction(fd, longLiteral());
            case FLOAT:
                return floatFunction(fd, floatLiteral());
            case DOUBLE:
                return doubleFunction(fd, doubleLiteral());
            case STRING:
                return castFunction(fd, Type.stringType());
            case ENUM:
                return castFunction(fd, ENUM_TYPE);
            case BYTE_STRING:
                return castFunction(fd, BYTE_STRING_TYPE).map(Protobuf::adapt, Type.byteType().arrayType());
            case MESSAGE:
                final MessageTypeParser parser = options.parsers().get(fd.getMessageType().getFullName());
                return parser == null ? null : parser.parse(fd, options);
            default:
                throw new IllegalStateException();
        }
    }

    private static TypedFunction<Message> repeatedTypedFunction(FieldDescriptor fd, ProtobufOptions options) {
        // we already know repeated, and not a map
        switch (fd.getJavaType()) {
            case BOOLEAN:
                return repeatedGenericFunction(fd, Boolean.class, b -> null, UNMAPPED_TYPE.arrayType());
            case INT:
                return repeatedIntFunction(fd, intLiteral());
            case LONG:
                return repeatedLongFunction(fd, longLiteral());
            case FLOAT:
                return repeatedFloatFunction(fd, floatLiteral());
            case DOUBLE:
                return repeatedDoubleFunction(fd, doubleLiteral());
            case STRING:
                return repeatedGenericFunction(fd, Type.stringType().arrayType());
            case ENUM:
                return repeatedGenericFunction(fd, ENUM_TYPE.arrayType());
            case BYTE_STRING:
                return repeatedGenericFunction(fd, ByteString.class, Protobuf::adapt, Type.byteType().arrayType().arrayType());
            case MESSAGE:
                final MessageTypeParser parser = options.parsers().get(fd.getMessageType().getFullName());
                // Note: we could consider repeating all of the individual types in this message in the future.
                // see SomeTest#repeatedMessageDescructured
                return parser == null ? null : parser.parseRepeated(fd, options);

            default:
                throw new IllegalStateException();
        }
    }

    private static BooleanFunction<Message> booleanFunction(FieldDescriptor fd, BooleanFunction<Object> toBoolean) {
        return m -> boolApply(m, fd, toBoolean);
    }

    private static IntFunction<Message> intFunction(FieldDescriptor fd, IntFunction<Object> toInt) {
        return m -> intApply(m, fd, toInt);
    }

    private static LongFunction<Message> longFunction(FieldDescriptor fd, LongFunction<Object> toLong) {
        return m -> longApply(m, fd, toLong);
    }

    private static FloatFunction<Message> floatFunction(FieldDescriptor fd, FloatFunction<Object> toFloat) {
        return m -> floatApply(m, fd, toFloat);
    }

    private static DoubleFunction<Message> doubleFunction(FieldDescriptor fd, DoubleFunction<Object> toDouble) {
        return m -> doubleApply(m, fd, toDouble);
    }

    private static ObjectFunction<Message, Message> messageFunction(FieldDescriptor fd) {
        return ObjectFunction.of(m -> objectApply(m, fd, castTo(MESSAGE_TYPE)).orElse(null), MESSAGE_TYPE);
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
        final TypedFunction<Message> keyTf = typedFunction(keyFd, options);
        if (keyTf == null) {
            return null;
        }
        final FieldDescriptor valueFd = fd.getMessageType().findFieldByNumber(2);
        final TypedFunction<Message> valueTf = typedFunction(valueFd, options);
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

    private static Boolean boolApply(Message m, FieldDescriptor fd, BooleanFunction<Object> toBoolean) {
        return hasField(m, fd)
                ? toBoolean.applyAsBoolean(m.getField(fd))
                : null;
    }

    private static int intApply(Message m, FieldDescriptor fd, IntFunction<Object> toInt) {
        return hasField(m, fd)
                ? toInt.applyAsInt(m.getField(fd))
                : QueryConstants.NULL_INT;
    }

    private static long longApply(Message m, FieldDescriptor fd, LongFunction<Object> toLong) {
        return hasField(m, fd)
                ? toLong.applyAsLong(m.getField(fd))
                : QueryConstants.NULL_LONG;
    }

    private static float floatApply(Message m, FieldDescriptor fd, FloatFunction<Object> toFloat) {
        return hasField(m, fd)
                ? toFloat.applyAsFloat(m.getField(fd))
                : QueryConstants.NULL_FLOAT;
    }

    private static double doubleApply(Message m, FieldDescriptor fd, DoubleFunction<Object> toDouble) {
        return hasField(m, fd)
                ? toDouble.applyAsDouble(m.getField(fd))
                : QueryConstants.NULL_DOUBLE;
    }

    private static <T> Optional<T> objectApply(Message m, FieldDescriptor fd, ObjectFunction<Object, T> toObject) {
        return hasField(m, fd)
                ? Optional.of(toObject.apply(m.getField(fd)))
                : Optional.empty();
    }

    // ----------------------------------------------------------------------------------------------------------------

    private static byte[] adapt(ByteString bs) {
        return bs == null ? null : bs.toByteArray();
    }

    private static <T, M extends Message> ObjectFunction<M, T> castFunction(FieldDescriptor fd, GenericType<T> type) {
        return ObjectFunction.of(m -> objectApply(m, fd, castTo(type)).orElse(null), type);
    }

    private static ObjectFunction<Message, int[]> repeatedIntFunction(FieldDescriptor fd,
            IntFunction<Object> toInt) {
        return ObjectFunction.of(m -> repeatedIntF(m, fd, toInt), Type.intType().arrayType());
    }

    private static ObjectFunction<Message, long[]> repeatedLongFunction(FieldDescriptor fd,
            LongFunction<Object> toLong) {
        return ObjectFunction.of(m -> repeatedLongF(m, fd, toLong), Type.longType().arrayType());
    }

    private static ObjectFunction<Message, float[]> repeatedFloatFunction(FieldDescriptor fd,
            FloatFunction<Object> toFloat) {
        return ObjectFunction.of(m -> repeatedFloatF(m, fd, toFloat), Type.floatType().arrayType());
    }

    private static ObjectFunction<Message, double[]> repeatedDoubleFunction(FieldDescriptor fd,
            DoubleFunction<Object> toDouble) {
        return ObjectFunction.of(m -> repeatedDoubleF(m, fd, toDouble), Type.doubleType().arrayType());
    }

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

    private static int[] repeatedIntF(Message m, FieldDescriptor fd, IntFunction<Object> toInt) {
        final int count = m.getRepeatedFieldCount(fd);
        int[] direct = new int[count];
        for (int i = 0; i < count; ++i) {
            direct[i] = toInt.applyAsInt(m.getRepeatedField(fd, i));
        }
        return direct;
    }

    private static long[] repeatedLongF(Message m, FieldDescriptor fd, LongFunction<Object> toLong) {
        final int count = m.getRepeatedFieldCount(fd);
        long[] direct = new long[count];
        for (int i = 0; i < count; ++i) {
            direct[i] = toLong.applyAsLong(m.getRepeatedField(fd, i));
        }
        return direct;
    }

    private static float[] repeatedFloatF(Message m, FieldDescriptor fd, FloatFunction<Object> toFloat) {
        final int count = m.getRepeatedFieldCount(fd);
        float[] direct = new float[count];
        for (int i = 0; i < count; ++i) {
            direct[i] = toFloat.applyAsFloat(m.getRepeatedField(fd, i));
        }
        return direct;
    }

    private static double[] repeatedDoubleF(Message m, FieldDescriptor fd, DoubleFunction<Object> toDouble) {
        final int count = m.getRepeatedFieldCount(fd);
        double[] direct = new double[count];
        for (int i = 0; i < count; ++i) {
            direct[i] = toDouble.applyAsDouble(m.getRepeatedField(fd, i));
        }
        return direct;
    }

    private static <T, R> R[] repeatedAdaptF(Message m, FieldDescriptor fd, Function<T, R> f, Class<T> clazz,
            Class<R> clazzR) {
        final int count = m.getRepeatedFieldCount(fd);
        // noinspection unchecked
        final R[] data = (R[]) Array.newInstance(clazzR, count);
        for (int i = 0; i < count; ++i) {
            data[i] = f.apply(repeatedObjectValue(m, fd, i, clazz));
        }
        return data;
    }

    private static <T, R> R[] repeatedAdaptF2(Message m, FieldDescriptor fd, ObjectFunction<T, R> f, Class<T> clazz,
            Class<R> clazzR) {
        final int count = m.getRepeatedFieldCount(fd);
        // noinspection unchecked
        final R[] data = (R[]) Array.newInstance(clazzR, count);
        for (int i = 0; i < count; ++i) {
            data[i] = f.apply(repeatedObjectValue(m, fd, i, clazz));
        }
        return data;
    }

    private static <T> T repeatedObjectValue(Message m, FieldDescriptor fd, int index, Class<T> clazz) {
        return clazz.cast(m.getRepeatedField(fd, index));
    }

    private static ObjectVector<Boolean> repeatedBooleanF(Message m, FieldDescriptor fd,
            BooleanFunction<Object> toBoolean) {
        final int count = m.getRepeatedFieldCount(fd);
        if (count == 0) {
            return BooleanVector.empty();
        }
        Boolean[] direct = new Boolean[count];
        for (int i = 0; i < count; ++i) {
            direct[i] = toBoolean.applyAsBoolean(m.getRepeatedField(fd, i));
        }
        return BooleanVector.proxy(new ObjectVectorDirect<>(direct));
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

    static List<MessageTypeParser> builtinParsers() {
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

    private enum TimestampParser implements MessageTypeParser, Function<Timestamp, Instant> {
        INSTANCE;

        private static final CustomType<Timestamp> IN_TYPE = Type.ofCustom(Timestamp.class);
        private static final InstantType OUT_TYPE = Type.instantType();

        @Override
        public String fullName() {
            return Timestamp.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return castFunction(fd, IN_TYPE).map(this, OUT_TYPE);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedGenericFunction(fd, IN_TYPE.clazz(), this, OUT_TYPE.arrayType());
        }

        @Override
        public Instant apply(Timestamp t) {
            return t == null ? null : Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
        }
    }

    private enum DurationParser implements MessageTypeParser, Function<com.google.protobuf.Duration, Duration> {
        INSTANCE;

        private static final CustomType<com.google.protobuf.Duration> IN_TYPE = Type.ofCustom(com.google.protobuf.Duration.class);
        private static final CustomType<Duration> OUT_TYPE = Type.ofCustom(Duration.class);

        @Override
        public String fullName() {
            return com.google.protobuf.Duration.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return castFunction(fd, IN_TYPE).map(this, OUT_TYPE);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedGenericFunction(fd, IN_TYPE.clazz(), this, OUT_TYPE.arrayType());
        }

        @Override
        public Duration apply(com.google.protobuf.Duration d) {
            return d == null ? null : Duration.ofSeconds(d.getSeconds(), d.getNanos());
        }
    }

    private enum BoolValueParser implements MessageTypeParser, BooleanFunction<Object>{
        INSTANCE;

        @Override
        public String fullName() {
            return BoolValue.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return booleanFunction(fd, this);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedGenericFunction(fd, BoolValue.class, b -> null, UNMAPPED_TYPE.arrayType());
        }

        @Override
        public Boolean applyAsBoolean(Object value) {
            return ((BoolValue) value).getValue();
        }
    }

    private enum Int32ValueParser implements MessageTypeParser, IntFunction<Object> {
        INSTANCE;

        @Override
        public String fullName() {
            return Int32Value.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return intFunction(fd, this);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedIntFunction(fd, this);
        }

        @Override
        public int applyAsInt(Object value) {
            return ((Int32Value) value).getValue();
        }
    }

    private enum UInt32ValueParser implements MessageTypeParser, IntFunction<Object> {
        INSTANCE;

        @Override
        public String fullName() {
            return UInt32Value.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return intFunction(fd, this);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedIntFunction(fd, this);
        }

        @Override
        public int applyAsInt(Object value) {
            return ((UInt32Value) value).getValue();
        }
    }

    private enum Int64ValueParser implements MessageTypeParser, LongFunction<Object> {
        INSTANCE;

        @Override
        public String fullName() {
            return Int64Value.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return longFunction(fd, this);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedLongFunction(fd, this);
        }

        @Override
        public long applyAsLong(Object value) {
            return ((Int64Value) value).getValue();
        }
    }

    private enum UInt64ValueParser implements MessageTypeParser, LongFunction<Object> {
        INSTANCE;

        @Override
        public String fullName() {
            return UInt64Value.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return longFunction(fd, this);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedLongFunction(fd, this);
        }

        @Override
        public long applyAsLong(Object value) {
            return ((UInt64Value) value).getValue();
        }
    }

    private enum FloatValueParser implements MessageTypeParser, FloatFunction<Object> {
        INSTANCE;

        @Override
        public String fullName() {
            return FloatValue.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return floatFunction(fd, this);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedFloatFunction(fd, this);
        }

        @Override
        public float applyAsFloat(Object value) {
            return ((FloatValue) value).getValue();
        }
    }

    private enum DoubleValueParser implements MessageTypeParser, DoubleFunction<Object> {
        INSTANCE;

        @Override
        public String fullName() {
            return DoubleValue.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return doubleFunction(fd, this);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedDoubleFunction(fd, this);
        }

        @Override
        public double applyAsDouble(Object value) {
            return ((DoubleValue) value).getValue();
        }
    }

    private enum StringValueParser implements MessageTypeParser, Function<StringValue, String> {
        INSTANCE;

        private static final CustomType<StringValue> IN_TYPE = Type.ofCustom(StringValue.class);
        private static final StringType OUT_TYPE = Type.stringType();

        @Override
        public String fullName() {
            return StringValue.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return castFunction(fd, IN_TYPE).map(this, OUT_TYPE);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedGenericFunction(fd, IN_TYPE.clazz(), this, OUT_TYPE.arrayType());
        }

        @Override
        public String apply(StringValue sv) {
            return sv == null ? null : sv.getValue();
        }
    }

    private enum BytesValueParser implements MessageTypeParser, Function<BytesValue, byte[]> {
        INSTANCE;

        private static final CustomType<BytesValue> IN_TYPE = Type.ofCustom(BytesValue.class);
        private static final NativeArrayType<byte[], Byte> OUT_TYPE = Type.byteType().arrayType();

        @Override
        public String fullName() {
            return BytesValue.getDescriptor().getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return castFunction(fd, IN_TYPE).map(this, OUT_TYPE);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedGenericFunction(fd, IN_TYPE.clazz(), this, OUT_TYPE.arrayType());
        }

        @Override
        public byte[] apply(BytesValue bv) {
            return bv == null ? null : bv.getValue().toByteArray();
        }
    }

    static <T extends Message> MessageTypeParser customParser(Class<T> clazz) {
        try {
            final Method method = clazz.getDeclaredMethod("getDescriptor");
            final Descriptor descriptor = (Descriptor) method.invoke(null);
            return new GenericParser<>(CustomType.of(clazz), descriptor);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static class GenericParser<T extends Message> implements MessageTypeParser {
        private final GenericType<T> type;
        private final Descriptor descriptor;

        public GenericParser(GenericType<T> type, Descriptor descriptor) {
            this.type = Objects.requireNonNull(type);
            this.descriptor = Objects.requireNonNull(descriptor);
        }

        @Override
        public String fullName() {
            return descriptor.getFullName();
        }

        @Override
        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
            return castFunction(fd, type);
        }

        @Override
        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
            return repeatedGenericFunction(fd, type.arrayType());
        }
    }
}
