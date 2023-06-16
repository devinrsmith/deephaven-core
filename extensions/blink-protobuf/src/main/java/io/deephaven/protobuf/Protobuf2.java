package io.deephaven.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.deephaven.protobuf.ProtobufFunctions.Builder;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Protobuf2 {

    public interface Context {
        Optional<ProtobufFunctions> wellKnown(Descriptor descriptor);

        Optional<ProtobufFunctions> wellKnown(FieldDescriptor fieldDescriptor);
    }

    private static ProtobufFunctions translate(Descriptor descriptor, Context context) {
        final ProtobufFunctions wellKnown = context.wellKnown(descriptor).orElse(null);
        if (wellKnown != null) {
            return wellKnown;
        }
        final Builder builder = ProtobufFunctions.builder();
        for (FieldDescriptor fd : descriptor.getFields()) {
            builder.putAllColumns(translate(fd, context).columns());
        }
        // todo: additional context, serialized size etc
        return builder.build();
    }

    private static ProtobufFunctions translate(FieldDescriptor fd, Context context) {
        final ProtobufFunctions wellKnown = context.wellKnown(fd).orElse(null);
        if (wellKnown != null) {
            return wellKnown;
        }
        return fieldObject(fd).toFunctions(context);
    }

    private static ProtobufFunctions simpleField(FieldDescriptor fd, TypedFunction<Message> f) {
        return ProtobufFunctions.builder().putColumns(List.of(fd.getName()), f).build();
    }

    private static List<String> prefix(String name, List<String> rest) {
        return Stream.concat(Stream.of(name), rest.stream()).collect(Collectors.toList());
    }

    private static FieldObject fieldObject(FieldDescriptor fd) {
        return new FieldObject(fd);
    }

    private static class FieldObject implements ObjectFunction<Message, Object> {
        private final FieldDescriptor fd;

        public FieldObject(FieldDescriptor fd) {
            this.fd = Objects.requireNonNull(fd);
        }

        @Override
        public GenericType<Object> returnType() {
            return Type.ofCustom(Object.class);
        }

        @Override
        public Object apply(Message value) {
            return Protobuf.hasField(value, fd) ? value.getField(fd) : null;
        }

        private ProtobufFunctions toFunctions(Context context) {
            switch (fd.getJavaType()) {
                case INT:
                    return simpleField(fd, mapInt(int_()));
                case LONG:
                    return simpleField(fd, mapLong(long_()));
                case FLOAT:
                    return simpleField(fd, mapFloat(float_()));
                case DOUBLE:
                    return simpleField(fd, mapDouble(double_()));
                case BOOLEAN:
                    return simpleField(fd, mapBoolean(boolean_()));
                case STRING:
                    return simpleField(fd, as(Type.stringType()));
                case BYTE_STRING:
                    return simpleField(fd, as(Type.ofCustom(ByteString.class)).mapObj(bytes_()));
                case ENUM:
                    return simpleField(fd, as(Type.ofCustom(EnumValueDescriptor.class)));
                case MESSAGE:
                    final ProtobufFunctions subF = translate(fd.getMessageType(), context);
                    final Function<Message, Message> toSubmessage = as(Type.ofCustom(Message.class))::apply;
                    final Builder builder = ProtobufFunctions.builder();
                    for (Entry<List<String>, TypedFunction<Message>> e : subF.columns().entrySet()) {
                        final List<String> key = e.getKey();
                        final TypedFunction<Message> value = e.getValue();
                        builder.putColumns(prefix(fd.getName(), key), value.mapInput(toSubmessage));
                    }
                    return builder.build();
                default:
                    throw new IllegalStateException();
            }
        }

        private static IntFunction<Object> int_() {
            return NullGuard.of((IntFunction<Object>) x -> (int) x);
        }

        private static LongFunction<Object> long_() {
            return NullGuard.of((LongFunction<Object>) x -> (long) x);
        }

        private static FloatFunction<Object> float_() {
            return NullGuard.of((FloatFunction<Object>) x -> (float) x);
        }

        private static DoubleFunction<Object> double_() {
            return NullGuard.of((DoubleFunction<Object>) x -> (double) x);
        }

        private static BooleanFunction<Object> boolean_() {
            return NullGuard.of((BooleanFunction<Object>) x -> (boolean) x);
        }

        private static ObjectFunction<ByteString, byte[]> bytes_() {
            return NullGuard.of(ObjectFunction.of(ByteString::toByteArray, Type.byteType().arrayType()));
        }
    }
}
