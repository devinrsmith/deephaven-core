package io.deephaven.blink;

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
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
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.BlinkTableMapper;
import io.deephaven.stream.blink.BlinkTableMapperConfig;
import io.deephaven.stream.blink.BlinkTableMapperConfig.Builder;
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
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;

public class Protobuf {

    private static final CustomType<Duration> DURATION_TYPE = Type.ofCustom(Duration.class);
    private static final CustomType<EnumValueDescriptor> ENUM_TYPE = Type.ofCustom(EnumValueDescriptor.class);
    private static final CustomType<Any> ANY_TYPE = Type.ofCustom(Any.class);
    private static final CustomType<FieldMask> FIELD_MASK_TYPE = Type.ofCustom(FieldMask.class);
    private static final CustomType<Message> MESSAGE_TYPE = Type.ofCustom(Message.class);
    private static final CustomType<ByteString> BYTE_STRING_TYPE = Type.ofCustom(ByteString.class);
    private static final CustomType<BytesValue> BYTES_VALUE_TYPE = Type.ofCustom(BytesValue.class);
    private static final CustomType<StringValue> STRING_VALUE_TYPE = Type.ofCustom(StringValue.class);
    private static final CustomType<Timestamp> TIMESTAMP_TYPE = Type.ofCustom(Timestamp.class);
    private static final CustomType<com.google.protobuf.Duration> PB_DURATION_TYPE =
            Type.ofCustom(com.google.protobuf.Duration.class);

    public static <M extends Message> BlinkTableMapper<M> create(Descriptor descriptor) {
        final Builder<M> builder = BlinkTableMapperConfig.<M>builder()
                .name(descriptor.getFullName())
                .chunkSize(1024)
                .updateSourceRegistrar(ExecutionContext.getContext().getUpdateGraph());
        for (Entry<String, TypedFunction<Message>> e : namedFunctions(descriptor).entrySet()) {
            // noinspection unchecked
            builder.putColumns(e.getKey(), (TypedFunction<M>) e.getValue());
        }
        return BlinkTableMapper.create(builder.build());
    }

    static Map<String, TypedFunction<Message>> namedFunctions(Descriptor descriptor) {
        final Map<String, TypedFunction<Message>> map = new LinkedHashMap<>();
        for (FieldDescriptor field : descriptor.getFields()) {
            map.putAll(namedFunctions(field));
        }
        return map;
    }

    private static Map<String, TypedFunction<Message>> namedFunctions(FieldDescriptor fd) {
        // todo
        final TypedFunction<Message> tf = typedFunction(fd);
        if (tf == null) {
            return Map.of();
        }
        final String name = fd.getName();
        if (!MESSAGE_TYPE.equals(tf.returnType())) {
            // "simple" type, return it
            return Map.of(name, tf);
        }
        // todo: parameter if we want to break it down?
        // let's recurse to break down the message:
        final Descriptor messageType = fd.getMessageType();
        final Map<String, TypedFunction<Message>> yolo = namedFunctions(messageType);
        final Map<String, TypedFunction<Message>> output = new LinkedHashMap<>(yolo.size());
        for (Entry<String, TypedFunction<Message>> e : yolo.entrySet()) {
            // todo: naming schema
            final String newName = name + "_" + e.getKey();
            final TypedFunction<Message> innerFunction = e.getValue();
            final TypedFunction<Message> adaptedFunction = innerFunction.mapInput(messageFunction(fd)::apply);
            output.put(newName, adaptedFunction);
        }
        return output;
    }

    private static TypedFunction<Message> typedFunction(FieldDescriptor fd) {
        if (fd.isMapField()) {
            return mapFunction(fd);
        }
        if (fd.isRepeated()) {
            return repeatedTypedFunction(fd);
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
                return castFunction(fd, BYTE_STRING_TYPE).map(Protobuf::adapt, ByteVector.type());
            case MESSAGE:
                switch (fd.getMessageType().getFullName()) {
                    case "google.protobuf.Timestamp":
                        return castFunction(fd, TIMESTAMP_TYPE).map(Protobuf::adapt, Type.instantType());
                    case "google.protobuf.Duration":
                        return castFunction(fd, PB_DURATION_TYPE).map(Protobuf::adapt, DURATION_TYPE);
                    case "google.protobuf.BoolValue":
                        return booleanFunction(fd, boolValue());
                    case "google.protobuf.Int32Value":
                        return intFunction(fd, int32Value());
                    case "google.protobuf.UInt32Value":
                        return intFunction(fd, uint32Value());
                    case "google.protobuf.Int64Value":
                        return longFunction(fd, int64Value());
                    case "google.protobuf.UInt64Value":
                        return longFunction(fd, uint64Value());
                    case "google.protobuf.FloatValue":
                        return floatFunction(fd, floatValue());
                    case "google.protobuf.DoubleValue":
                        return doubleFunction(fd, doubleValue());
                    case "google.protobuf.StringValue":
                        return castFunction(fd, STRING_VALUE_TYPE).map(Protobuf::adapt, Type.stringType());
                    case "google.protobuf.BytesValue":
                        return castFunction(fd, BYTES_VALUE_TYPE).map(Protobuf::adapt, ByteVector.type());
                    case "google.protobuf.Any":
                        return castFunction(fd, ANY_TYPE);
                    case "google.protobuf.FieldMask":
                        return castFunction(fd, FIELD_MASK_TYPE);
                    default:
                        return castFunction(fd, MESSAGE_TYPE);
                }
            default:
                throw new IllegalStateException();
        }
    }

    private static ObjectFunction<Message, ?> repeatedTypedFunction(FieldDescriptor fd) {
        // we already know repeated, and not a map
        switch (fd.getJavaType()) {
            case BOOLEAN:
                return null;
                //return repeatedBooleanFunction(fd, booleanLiteral());
            case INT:
                return repeatedIntFunction(fd, intLiteral());
            case LONG:
                return repeatedLongFunction(fd, longLiteral());
            case FLOAT:
                return repeatedFloatFunction(fd, floatLiteral());
            case DOUBLE:
                return repeatedDoubleFunction(fd, doubleLiteral());
            case STRING:
                return repeatedGenericFunction(fd, Type.stringType());
            case ENUM:
                return repeatedGenericFunction(fd, ENUM_TYPE);
            case BYTE_STRING:
                return repeatedGenericFunction(fd, ByteVector.type(), ByteString.class, Protobuf::adapt);
            case MESSAGE:
                switch (fd.getMessageType().getFullName()) {
                    case "google.protobuf.Timestamp":
                        return repeatedGenericFunction(fd, Type.instantType(), Timestamp.class, Protobuf::adapt);
                    case "google.protobuf.Duration":
                        return repeatedGenericFunction(fd, DURATION_TYPE, com.google.protobuf.Duration.class,
                                Protobuf::adapt);
                    case "google.protobuf.BoolValue":
                        return null;
                        //return repeatedBooleanFunction(fd, boolValue());
                    case "google.protobuf.Int32Value":
                        return repeatedIntFunction(fd, int32Value());
                    case "google.protobuf.UInt32Value":
                        return repeatedIntFunction(fd, uint32Value());
                    case "google.protobuf.Int64Value":
                        return repeatedLongFunction(fd, int64Value());
                    case "google.protobuf.UInt64Value":
                        return repeatedLongFunction(fd, uint64Value());
                    case "google.protobuf.FloatValue":
                        return repeatedFloatFunction(fd, floatValue());
                    case "google.protobuf.DoubleValue":
                        return repeatedDoubleFunction(fd, doubleValue());
                    case "google.protobuf.StringValue":
                        return repeatedGenericFunction(fd, Type.stringType(), StringValue.class, Protobuf::adapt);
                    case "google.protobuf.BytesValue":
                        return repeatedGenericFunction(fd, ByteVector.type(), BytesValue.class, Protobuf::adapt);
                    case "google.protobuf.Any":
                        return repeatedGenericFunction(fd, ANY_TYPE);
                    case "google.protobuf.FieldMask":
                        return repeatedGenericFunction(fd, FIELD_MASK_TYPE);
                    default:
                        return repeatedGenericFunction(fd, MESSAGE_TYPE);
                }
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


    private static ObjectFunction<Message, Map> mapFunction(FieldDescriptor fd) {
        final FieldDescriptor keyFd = fd.getMessageType().findFieldByNumber(1);
        final FieldDescriptor valueFd = fd.getMessageType().findFieldByNumber(2);
        final TypedFunction<Message> keyTf = typedFunction(keyFd);
        if (keyTf == null) {
            return null;
        }
        final TypedFunction<Message> valueTf = typedFunction(valueFd);
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

    private static Instant adapt(Timestamp t) {
        return t == null ? null : Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
    }

    private static Duration adapt(com.google.protobuf.Duration d) {
        return d == null ? null : Duration.ofSeconds(d.getSeconds(), d.getNanos());
    }

    private static ByteVector adapt(ByteString bs) {
        return bs == null ? null : new ByteVectorByteStringWrapper(bs);
    }

    private static ByteVector adapt(BytesValue bv) {
        return bv == null ? null : new ByteVectorByteStringWrapper(bv.getValue());
    }

    private static String adapt(StringValue sv) {
        return sv == null ? null : sv.getValue();
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


    private static <T> ObjectFunction<Message, T[]> repeatedGenericFunction(FieldDescriptor fd,
            GenericType<T> type) {
        return repeatedGenericFunction(fd, type, type.clazz(), Function.identity());
    }

    private static <T, R> ObjectFunction<Message, R[]> repeatedGenericFunction(
            FieldDescriptor fd,
            Class<T> intermediateType,
            Class<R> componentType,
            Function<T, R> adapter) {
        final GenericType<R[]> returnType = null;
        return ObjectFunction.of(m -> {
            final int count = m.getRepeatedFieldCount(fd);
            //noinspection unchecked
            final R[] data = (R[]) Array.newInstance(componentType, count);
            for (int i = 0; i < count; ++i) {
                data[i] = adapter.apply(repeatedObjectValue(m, fd, i, intermediateType));
            }
            return data;
        }, returnType);

        /*
        final GenericType<R[]> returnType = null;
        final NativeArrayType<?, R> arrayType = componentType.arrayType();

        final ObjectFunction<Object, int[]> objectObjectFunction = ObjectFunction.of2(null, Type.intType().arrayType());

        final ObjectFunction<Object, int[]> of = ObjectFunction.of(null, Type.intType().arrayType());

        return ObjectFunction.of(message -> repeatedAdaptF(message, fd, adapter, intermediateType, componentType.clazz()), returnType);*/
    }

//    private static ObjectFunction<Message, Boolean[]> repeatedBooleanFunction(
//            FieldDescriptor fd,
//            BooleanFunction<Object> toBoolean) {
//        final GenericType<ObjectVector<Boolean>> returnType = ;
//        return ObjectFunction.of(m -> repeatedBooleanF(m, fd, toBoolean), returnType);
//    }

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

    private static <T, R> R[] repeatedAdaptF(Message m, FieldDescriptor fd, Function<T, R> f, Class<T> clazz, Class<R> clazzR) {
        final int count = m.getRepeatedFieldCount(fd);
        //noinspection unchecked
        final R[] data = (R[]) Array.newInstance(clazzR, count);
        for (int i = 0; i < count; ++i) {
            data[i] = f.apply(repeatedObjectValue(m, fd, i, clazz));
        }
        return data;
    }

    private static <T, R> R[] repeatedAdaptF2(Message m, FieldDescriptor fd, ObjectFunction<T, R> f, Class<T> clazz, Class<R> clazzR) {
        final int count = m.getRepeatedFieldCount(fd);
        //noinspection unchecked
        final R[] data = (R[]) Array.newInstance(clazzR, count);
        for (int i = 0; i < count; ++i) {
            data[i] = f.apply(repeatedObjectValue(m, fd, i, clazz));
        }
        return data;
    }

    private static <T> T repeatedObjectValue(Message m, FieldDescriptor fd, int index, Class<T> clazz) {
        return clazz.cast(m.getRepeatedField(fd, index));
    }

    private static ObjectVector<Boolean> repeatedBooleanF(Message m, FieldDescriptor fd, BooleanFunction<Object> toBoolean) {
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

    private static BooleanFunction<Object> boolValue() {
        return o -> ((BoolValue) o).getValue();
    }

    private static IntFunction<Object> int32Value() {
        return o -> ((Int32Value) o).getValue();
    }

    private static IntFunction<Object> uint32Value() {
        return o -> ((UInt32Value) o).getValue();
    }

    private static LongFunction<Object> int64Value() {
        return o -> ((Int64Value) o).getValue();
    }

    private static LongFunction<Object> uint64Value() {
        return o -> ((UInt64Value) o).getValue();
    }

    private static FloatFunction<Object> floatValue() {
        return o -> ((FloatValue) o).getValue();
    }

    private static DoubleFunction<Object> doubleValue() {
        return o -> ((DoubleValue) o).getValue();
    }
}
