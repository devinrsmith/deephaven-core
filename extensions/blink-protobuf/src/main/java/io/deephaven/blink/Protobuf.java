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
import io.deephaven.vector.ByteVector;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class Protobuf {

    public static <M extends Message> BlinkTableMapper<M> create(Descriptor descriptor) {
        final Builder<M> builder = BlinkTableMapperConfig.<M>builder()
                .name(descriptor.getFullName())
                .chunkSize(1024)
                .updateSourceRegistrar(ExecutionContext.getContext().getUpdateGraph());
        for (FieldDescriptor field : descriptor.getFields()) {
            final TypedFunction<Message> tf = typedFunction(field);
            if (tf != null) {
                // noinspection unchecked
                builder.putColumns(field.getName(), (TypedFunction<M>) tf);
            }
        }
        return BlinkTableMapper.create(builder.build());
    }

    public static TypedFunction<Message> typedFunction(FieldDescriptor fd) {
        if (fd.isMapField()) {
            return mapFunction(fd);
        }
        if (fd.isRepeated()) {
            return null;
        }
        switch (fd.getJavaType()) {
            case BOOLEAN:
                return booleanFunction(fd);
            case INT:
                return intFunction(fd);
            case LONG:
                return longFunction(fd);
            case FLOAT:
                return floatFunction(fd);
            case DOUBLE:
                return doubleFunction(fd);
            case STRING:
                return stringFunction(fd);
            case ENUM:
                return customTypedFunction(fd, EnumValueDescriptor.class);
            case BYTE_STRING:
                return byteVectorFunction(fd);
            case MESSAGE:
                switch (fd.getMessageType().getFullName()) {
                    case "google.protobuf.Timestamp":
                        return timestampFunction(fd);
                    case "google.protobuf.Duration":
                        return durationFunction(fd);
                    case "google.protobuf.BoolValue":
                        return booleanValueFunction(fd);
                    case "google.protobuf.Int32Value":
                        return int32ValueFunction(fd);
                    case "google.protobuf.UInt32Value":
                        return uint32ValueFunction(fd);
                    case "google.protobuf.Int64Value":
                        return int64ValueFunction(fd);
                    case "google.protobuf.UInt64Value":
                        return uint64ValueFunction(fd);
                    case "google.protobuf.FloatValue":
                        return floatValueFunction(fd);
                    case "google.protobuf.DoubleValue":
                        return doubleValueFunction(fd);
                    case "google.protobuf.StringValue":
                        return stringValueFunction(fd);
                    case "google.protobuf.BytesValue":
                        return byteVectorValueFunction(fd);
                    case "google.protobuf.Any":
                        return customTypedFunction(fd, Any.class);
                    case "google.protobuf.FieldMask":
                        return customTypedFunction(fd, FieldMask.class);
                    default:
                        // todo recursive?
                        return customTypedFunction(fd, Message.class);
                }
                // todo: well known message types (timestamp, etc)
                // todo Any
                // throw new IllegalArgumentException();
            default:
                throw new IllegalStateException();
        }
    }

    private static BooleanFunction<Message> booleanFunction(FieldDescriptor fd) {
        return value -> boolValue(value, fd);
    }

    private static BooleanFunction<Message> booleanValueFunction(FieldDescriptor fd) {
        return value -> boolValueWrapper(value, fd);
    }

    private static IntFunction<Message> intFunction(FieldDescriptor fd) {
        return value -> intValue(value, fd);
    }

    private static IntFunction<Message> int32ValueFunction(FieldDescriptor fd) {
        return value -> int32ValueWrapper(value, fd);
    }

    private static IntFunction<Message> uint32ValueFunction(FieldDescriptor fd) {
        return value -> uint32ValueWrapper(value, fd);
    }

    private static LongFunction<Message> longFunction(FieldDescriptor fd) {
        return value -> longValue(value, fd);
    }

    private static LongFunction<Message> int64ValueFunction(FieldDescriptor fd) {
        return value -> int64ValueWrapper(value, fd);
    }

    private static LongFunction<Message> uint64ValueFunction(FieldDescriptor fd) {
        return value -> uint64ValueWrapper(value, fd);
    }

    private static FloatFunction<Message> floatFunction(FieldDescriptor fd) {
        return value -> floatValue(value, fd);
    }

    private static FloatFunction<Message> floatValueFunction(FieldDescriptor fd) {
        return value -> floatValueWrapper(value, fd);
    }

    private static DoubleFunction<Message> doubleFunction(FieldDescriptor fd) {
        return value -> doubleValue(value, fd);
    }

    private static DoubleFunction<Message> doubleValueFunction(FieldDescriptor fd) {
        return value -> doubleValueWrapper(value, fd);
    }

    private static ObjectFunction<Message, String> stringFunction(FieldDescriptor fd) {
        // todo: should string be special case? treat empty as null by default?
        return ObjectFunction.of(value -> objectValue(value, fd, String.class), Type.stringType());
    }

    private static ObjectFunction<Message, String> stringValueFunction(FieldDescriptor fd) {
        return ObjectFunction.of(value -> stringValueWrapper(value, fd), Type.stringType());
    }

    private static ObjectFunction<Message, ByteVector> byteVectorFunction(FieldDescriptor fd) {
        return ObjectFunction.of(value -> {
            final ByteString bytes = objectValue(value, fd, ByteString.class);
            if (bytes == null) {
                return null;
            }
            return new ByteVectorByteStringWrapper(bytes);
        }, ByteVector.type());
    }

    private static ObjectFunction<Message, ByteVector> byteVectorValueFunction(FieldDescriptor fd) {
        return ObjectFunction.of(m -> {
            final BytesValue bytes = objectValue(m, fd, BytesValue.class);
            if (bytes == null) {
                return null;
            }
            return new ByteVectorByteStringWrapper(bytes.getValue());
        }, ByteVector.type());
    }

    private static ObjectFunction<Message, Instant> timestampFunction(FieldDescriptor fd) {
        return ObjectFunction.of(v -> timestampValue(v, fd), Type.instantType());
    }

    private static ObjectFunction<Message, Duration> durationFunction(FieldDescriptor fd) {
        return ObjectFunction.of(v -> durationValue(v, fd), CustomType.of(Duration.class));
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
        }, CustomType.of(Map.class));
    }

    private static Boolean boolValue(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? (boolean) value.getField(fd)
                : null;
    }

    private static Boolean boolValueWrapper(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? ((BoolValue) value.getField(fd)).getValue()
                : null;
    }

    private static boolean hasField(Message value, FieldDescriptor fd) {
        return !fd.hasPresence() || value.hasField(fd);
    }

    private static int intValue(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? (int) value.getField(fd)
                : QueryConstants.NULL_INT;
    }

    private static int int32ValueWrapper(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? ((Int32Value) value.getField(fd)).getValue()
                : QueryConstants.NULL_INT;
    }

    private static int uint32ValueWrapper(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? ((UInt32Value) value.getField(fd)).getValue()
                : QueryConstants.NULL_INT;
    }

    private static long longValue(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? (long) value.getField(fd)
                : QueryConstants.NULL_LONG;
    }

    private static long int64ValueWrapper(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? ((Int64Value) value.getField(fd)).getValue()
                : QueryConstants.NULL_LONG;
    }

    private static long uint64ValueWrapper(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? ((UInt64Value) value.getField(fd)).getValue()
                : QueryConstants.NULL_LONG;
    }

    private static float floatValue(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? (float) value.getField(fd)
                : QueryConstants.NULL_FLOAT;
    }

    private static float floatValueWrapper(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? ((FloatValue) value.getField(fd)).getValue()
                : QueryConstants.NULL_FLOAT;
    }

    private static double doubleValue(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? (double) value.getField(fd)
                : QueryConstants.NULL_DOUBLE;
    }

    private static double doubleValueWrapper(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? ((DoubleValue) value.getField(fd)).getValue()
                : QueryConstants.NULL_DOUBLE;
    }

    private static <T> T objectValue(Message value, FieldDescriptor fd, Class<T> clazz) {
        return hasField(value, fd)
                ? clazz.cast(value.getField(fd))
                : null;
    }

    private static String stringValueWrapper(Message value, FieldDescriptor fd) {
        return hasField(value, fd)
                ? ((StringValue) value.getField(fd)).getValue()
                : null;
    }

    private static Instant timestampValue(Message value, FieldDescriptor fd) {
        final Timestamp timestamp = objectValue(value, fd, Timestamp.class);
        if (timestamp == null) {
            return null;
        }
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static Duration durationValue(Message value, FieldDescriptor fd) {
        final com.google.protobuf.Duration duration =
                objectValue(value, fd, com.google.protobuf.Duration.class);
        if (duration == null) {
            return null;
        }
        return Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
    }

    private static <T, M extends Message> ObjectFunction<M, T> customTypedFunction(FieldDescriptor fd,
            Class<T> clazz) {
        return ObjectFunction.of(value -> objectValue(value, fd, clazz), CustomType.of(clazz));
    }
}
