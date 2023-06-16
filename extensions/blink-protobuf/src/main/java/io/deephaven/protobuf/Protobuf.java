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
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.GenericType.Visitor;
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

    public static Map<List<String>, TypedFunction<Message>> parser(Descriptor descriptor, ProtobufOptions options) {
        return namedFunctions(descriptor, options, List.of());
    }

    private static Map<List<String>, TypedFunction<Message>> namedFunctions(Descriptor descriptor,
            ProtobufOptions options, List<String> context) {
        final Map<List<String>, TypedFunction<Message>> map = new LinkedHashMap<>();
        for (FieldDescriptor field : descriptor.getFields()) {
            map.putAll(namedFunctions(field, options, context));
        }
        options.serializedSizeName(descriptor, options, context).ifPresent(name -> map.put(path(context, name), serializedSizeFunction()));
        options.unknownFieldSetName(descriptor, options, context).ifPresent(name -> map.put(path(context, name), unknownFieldsFunction()));
        options.rawMessageName(descriptor, options, context).ifPresent(name -> map.put(path(context, name), rawMessageFunction()));
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

        // All simple and known messages types should already be handled

        if (fd.getJavaType() != JavaType.MESSAGE) {
            throw new IllegalStateException();
        }

        if (!fd.isRepeated()) {
            return options.parseAdHocMessage(fd, options, context)
                    ? singularMessageFunctions(fd, options, fieldPath)
                    : Map.of(fieldPath, castFunction(fd, MESSAGE_TYPE));
        } else {
            return options.parseAdHocRepeatedMessage(fd, options, context)
                    ? repeatedMessageFunctions(fd, options, fieldPath)
                    : Map.of(fieldPath, repeatedGenericFunction(fd, MESSAGE_TYPE.arrayType()));
        }
    }

    private static Map<List<String>, TypedFunction<Message>> singularMessageFunctions(FieldDescriptor fd, ProtobufOptions options, List<String> fieldPath) {
        final Descriptor messageType = fd.getMessageType();
        final Function<Message, Message> messageFunction = messageFunction(fd)::apply;
        final Map<List<String>, TypedFunction<Message>> nf = namedFunctions(messageType, options, fieldPath);
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

    private static TypedFunction<Message> typedFunction(FieldDescriptor fd, ProtobufOptions options) {
        if (fd.isMapField()) {
            // special case? todo, make optional
            return mapFunction(fd, options);
        }
        return !fd.isRepeated()
                ? singleMessageToValue(fd, options)
                : repeatedMessageToValue2(fd, options);
    }

    private static TypedFunction<Message> singleMessageToValue(FieldDescriptor fd, ProtobufOptions options) {
        return messageToValue(fieldFunction(fd), fd, options);
    }

    private static ObjectFunction<Message, ?> repeatedMessageToValue2(FieldDescriptor fd, ProtobufOptions options) {
        final int[] index = new int[] { 0 };
        final TypedFunction<Message> tf = messageToValue(ObjectFunction.of(m -> m.getRepeatedField(fd, index[0]), CustomType.of(Object.class)), fd, options);
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

    private static TypedFunction<Message> messageToValue(ObjectFunction<Message, Object> messageToObject, FieldDescriptor fd, ProtobufOptions options) {
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
                    return null;
                }
                return messageToObject.as(MESSAGE_TYPE).map(NullGuard.of(parser.parser(options)));
            default:
                throw new IllegalStateException();
        }
    }


    private static TypedFunction<Message> repeatedMessageToValue(FieldDescriptor fd, ProtobufOptions options) {

        final TypedFunction<Message> valueFunction = messageToValue(null, fd, options);

        // we already know repeated, and not a map
        switch (fd.getJavaType()) {
            case BOOLEAN:
                return repeatedGenericFunction(fd, Boolean.class, b -> null, UNMAPPED_TYPE.arrayType());
            case INT:


                //return repeatedIntFunction(fd, intLiteral());
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
                return repeatedGenericFunction(fd, BYTE_STRING_TYPE.clazz(), Protobuf::adapt, Type.byteType().arrayType().arrayType());
            case MESSAGE:
                final SingleValuedMessageParser parser = options.parsers().get(fd.getMessageType().getFullName());
                // Note: we could consider repeating all of the individual types in this message in the future.
                // see SomeTest#repeatedMessageDescructured
                //return parser == null ? null : parser.parseRepeated(fd, options);
                return null;

            default:
                throw new IllegalStateException();
        }
    }

    private static <T> TypedFunction<T> nullGuard(TypedFunction<T> what) {
        return NullGuard.of(what);
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

    private static ObjectFunction<Message, Object> fieldFunctionRepeated(FieldDescriptor fd, int index) {
        return ObjectFunction.of(message -> message.getRepeatedField(fd, index), Type.ofCustom(Object.class));
    }

    private static ObjectFunction<Message, Message> messageFieldFunction(FieldDescriptor fd) {
        return fieldFunction(fd).as(MESSAGE_TYPE);
    }

    private static ObjectFunction<Message, Message> messageFieldFunctionRepeated(FieldDescriptor fd, int index) {
        return fieldFunctionRepeated(fd, index).as(MESSAGE_TYPE);
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

//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return messageFieldFunction(fd).map(parser(options));
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedGenericFunction(fd, IN_TYPE.clazz(), this, OUT_TYPE.arrayType());
//        }

        @Override
        public Instant apply(Timestamp t) {
            return t == null ? null : Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
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

//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return messageFieldFunction(fd).map(parser(options));
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedGenericFunction(fd, IN_TYPE.clazz(), this, OUT_TYPE.arrayType());
//        }

        @Override
        public Duration apply(com.google.protobuf.Duration d) {
            return d == null ? null : Duration.ofSeconds(d.getSeconds(), d.getNanos());
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

//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return messageFieldFunction(fd).mapToBoolean(parser(options));
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            // todo
//            return repeatedGenericFunction(fd, BoolValue.class, b -> null, UNMAPPED_TYPE.arrayType());
//        }
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

//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return messageFieldFunction(fd).mapToInt(parser(options));
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedIntFunction(fd, this);
//        }
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

//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return messageFieldFunction(fd).mapToInt(parser(options));
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedIntFunction(fd, this);
//        }
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


//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return messageFieldFunction(fd).mapToLong(parser(options));
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedLongFunction(fd, this);
//        }
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


//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return longFunction(fd, this);
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedLongFunction(fd, this);
//        }
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


//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return floatFunction(fd, this);
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedFloatFunction(fd, this);
//        }
//
//        @Override
//        public float applyAsFloat(Object value) {
//            return ((FloatValue) value).getValue();
//        }
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

//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return doubleFunction(fd, this);
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedDoubleFunction(fd, this);
//        }
//
//        @Override
//        public double applyAsDouble(Object value) {
//            return ((DoubleValue) value).getValue();
//        }
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


//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return castFunction(fd, IN_TYPE).map(this, OUT_TYPE);
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedGenericFunction(fd, IN_TYPE.clazz(), this, OUT_TYPE.arrayType());
//        }
//
//        @Override
//        public String apply(StringValue sv) {
//            return sv == null ? null : sv.getValue();
//        }
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


//        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return castFunction(fd, IN_TYPE).map(this, OUT_TYPE);
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedGenericFunction(fd, IN_TYPE.clazz(), this, OUT_TYPE.arrayType());
//        }
//
//        @Override
//        public byte[] apply(BytesValue bv) {
//            return bv == null ? null : bv.getValue().toByteArray();
//        }
    }

    static <T extends Message> SingleValuedMessageParser customParser(Class<T> clazz) {
        try {
            final Method method = clazz.getDeclaredMethod("getDescriptor");
            final Descriptor descriptor = (Descriptor) method.invoke(null);
            return new GenericParser<>(CustomType.of(clazz), descriptor);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static class GenericParser<T extends Message> implements SingleValuedMessageParser {
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
        public TypedFunction<Message> parser(ProtobufOptions options) {
            return message(type);
        }

        //        @Override
//        public TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options) {
//            return castFunction(fd, type);
//        }
//
//        @Override
//        public TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options) {
//            return repeatedGenericFunction(fd, type.arrayType());
//        }
    }

    private static TypedFunction<Message> toArray(FieldDescriptor fd, TypedFunction<Object> tf) {
        return tf.walk(new ToArrayTypedFunction(fd));
    }

    private static class ToArrayTypedFunction implements TypedFunction.Visitor<Object, TypedFunction<Message>> {

        private final FieldDescriptor fd;

        public ToArrayTypedFunction(FieldDescriptor fd) {
            this.fd = Objects.requireNonNull(fd);
        }

        @Override
        public TypedFunction<Message> visit(BooleanFunction<Object> f) {
            // todo
            return repeatedGenericFunction(fd, Boolean.class, b -> null, UNMAPPED_TYPE.arrayType());
        }

        @Override
        public TypedFunction<Message> visit(CharFunction<Object> f) {
            return ObjectFunction.of(m -> toChars(fd, f, m), f.returnType().arrayType());
        }

        @Override
        public TypedFunction<Message> visit(ByteFunction<Object> f) {
            return ObjectFunction.of(m -> toBytes(fd, f, m), f.returnType().arrayType());
        }

        @Override
        public TypedFunction<Message> visit(ShortFunction<Object> f) {
            return ObjectFunction.of(m -> toShorts(fd, f, m), f.returnType().arrayType());
        }

        @Override
        public TypedFunction<Message> visit(IntFunction<Object> f) {
            return ObjectFunction.of(m -> toInts(fd, f, m), f.returnType().arrayType());
        }

        @Override
        public TypedFunction<Message> visit(LongFunction<Object> f) {
            return ObjectFunction.of(m -> toLongs(fd, f, m), f.returnType().arrayType());
        }

        @Override
        public TypedFunction<Message> visit(FloatFunction<Object> f) {
            return ObjectFunction.of(m -> toFloats(fd, f, m), f.returnType().arrayType());
        }

        @Override
        public TypedFunction<Message> visit(DoubleFunction<Object> f) {
            return ObjectFunction.of(m -> toDoubles(fd, f, m), f.returnType().arrayType());
        }

        @Override
        public TypedFunction<Message> visit(ObjectFunction<Object, ?> f) {

//            NativeArrayType<Object[], Object> x = f.arrayType();
//            out[0] = ObjectFunction.of(message -> {
//                //noinspection unchecked,rawtypes
//                return toGenericArray(fd, (ObjectFunction) f, message, customType.clazz());
//            }, x);


            final TypedFunction<Message>[] out = new TypedFunction[1];

//            out[0] = ObjectFunction.of(m -> toGenericArray(fd, (ObjectFunction)f, m, f.returnType().clazz()), f.returnType().arrayType());

            f.returnType().walk(new Visitor() {
                @Override
                public void visit(StringType stringType) {
                    out[0] = ObjectFunction.of(m -> {
                        //noinspection unchecked
                        return toGenericArray(fd, (ObjectFunction<Object, String>) f, m, String.class);
                    }, stringType.arrayType());
                }

                @Override
                public void visit(InstantType instantType) {
                    out[0] = ObjectFunction.of(m -> {
                        //noinspection unchecked
                        return toGenericArray(fd, (ObjectFunction<Object, Instant>) f, m, Instant.class);
                    }, instantType.arrayType());
                }

                @Override
                public void visit(ArrayType<?, ?> arrayType) {
                    //noinspection unchecked
//                    final NativeArrayType<Object[], Object> at2  = ((ArrayType<Object, ?>) arrayType).arrayType();
//                    out[0] = ObjectFunction.of(message -> {
//                        //noinspection unchecked,rawtypes
//                        return toGenericArray(fd, (ObjectFunction) f, message, arrayType.clazz());
//                    }, at2);
                }

                @Override
                public void visit(CustomType<?> customType) {
                    //noinspection unchecked
                    NativeArrayType<Object[], Object> arrayType = ((CustomType<Object>)customType).arrayType();
                    out[0] = ObjectFunction.of(message -> {
                        //noinspection unchecked,rawtypes
                        return toGenericArray(fd, (ObjectFunction) f, message, customType.clazz());
                    }, arrayType);
                }
            });

            return out[0];
        }

        private static char[] toChars(FieldDescriptor fd, CharFunction<Object> f, Message message) {
            final int count = message.getRepeatedFieldCount(fd);
            final char[] out = new char[count];
            for (int i = 0; i < count; ++i) {
                out[i] = f.applyAsChar(message.getRepeatedField(fd, i));
            }
            return out;
        }

        private static byte[] toBytes(FieldDescriptor fd, ByteFunction<Object> f, Message message) {
            final int count = message.getRepeatedFieldCount(fd);
            final byte[] out = new byte[count];
            for (int i = 0; i < count; ++i) {
                out[i] = f.applyAsByte(message.getRepeatedField(fd, i));
            }
            return out;
        }

        private static short[] toShorts(FieldDescriptor fd, ShortFunction<Object> f, Message message) {
            final int count = message.getRepeatedFieldCount(fd);
            final short[] out = new short[count];
            for (int i = 0; i < count; ++i) {
                out[i] = f.applyAsShort(message.getRepeatedField(fd, i));
            }
            return out;
        }

        private static int[] toInts(FieldDescriptor fd, IntFunction<Object> f, Message message) {
            final int count = message.getRepeatedFieldCount(fd);
            final int[] out = new int[count];
            for (int i = 0; i < count; ++i) {
                out[i] = f.applyAsInt(message.getRepeatedField(fd, i));
            }
            return out;
        }

        private static long[] toLongs(FieldDescriptor fd, LongFunction<Object> f, Message message) {
            final int count = message.getRepeatedFieldCount(fd);
            final long[] out = new long[count];
            for (int i = 0; i < count; ++i) {
                out[i] = f.applyAsLong(message.getRepeatedField(fd, i));
            }
            return out;
        }

        private static float[] toFloats(FieldDescriptor fd, FloatFunction<Object> f, Message message) {
            final int count = message.getRepeatedFieldCount(fd);
            final float[] out = new float[count];
            for (int i = 0; i < count; ++i) {
                out[i] = f.applyAsFloat(message.getRepeatedField(fd, i));
            }
            return out;
        }

        private static double[] toDoubles(FieldDescriptor fd, DoubleFunction<Object> f, Message message) {
            final int count = message.getRepeatedFieldCount(fd);
            final double[] out = new double[count];
            for (int i = 0; i < count; ++i) {
                out[i] = f.applyAsDouble(message.getRepeatedField(fd, i));
            }
            return out;
        }

        private static <R> R[] toGenericArray(FieldDescriptor fd, ObjectFunction<Object, R> f, Message message, Class<R> clazz) {
            final int count = message.getRepeatedFieldCount(fd);
            //noinspection unchecked
            final R[] out = (R[]) Array.newInstance(clazz, count);
            for (int i = 0; i < count; ++i) {
                out[i] = f.apply(message.getRepeatedField(fd, i));
            }
            return out;
        }
    }
}
