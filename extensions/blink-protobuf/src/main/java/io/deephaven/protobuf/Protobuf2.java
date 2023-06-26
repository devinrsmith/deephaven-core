package io.deephaven.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
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

    private final ProtobufOptions options;

    public Protobuf2(ProtobufOptions options) {
        this.options = Objects.requireNonNull(options);
    }

    public ProtobufFunctions translate(Descriptor descriptor) {
        return new DescriptorContext(List.of(), descriptor).functions();
    }

    private static List<String> prefix(String name, List<String> rest) {
        return Stream.concat(Stream.of(name), rest.stream()).collect(Collectors.toList());
    }

    private TypedFunction<Message> parser(SingleValuedMessageParser svmp) {
        return NullGuard.of(svmp.parser(options));
    }

    private class DescriptorContext {
        private final List<FieldDescriptor> parents;
        private final Descriptor descriptor;

        public DescriptorContext(List<FieldDescriptor> parents, Descriptor descriptor) {
            this.parents = Objects.requireNonNull(parents);
            this.descriptor = Objects.requireNonNull(descriptor);
        }

        private ProtobufFunctions functions() {
            final ProtobufFunctions wellKnown = wellKnown().orElse(null);
            if (wellKnown != null) {
                return wellKnown;
            }
            final Builder builder = ProtobufFunctions.builder();
            for (FieldContext fc : fcs()) {
                builder.putAllColumns(fc.functions().columns());
            }
            // todo: additional context, serialized size etc
            return builder.build();
        }

        private Optional<ProtobufFunctions> wellKnown() {
            // todo: eventually support cases that are >1 field
            return svmp()
                    .map(Protobuf2.this::parser)
                    .map(ProtobufFunctions::unnamed);
        }

        private Optional<SingleValuedMessageParser> svmp() {
            return Optional.ofNullable(options.parsers().get(descriptor.getFullName()));
        }

        private List<FieldContext> fcs() {
            return descriptor.getFields().stream().map(this::fc).collect(Collectors.toList());
        }

        private FieldContext fc(FieldDescriptor fd) {
            return new FieldContext(this, fd);
        }
    }

    private class FieldContext {
        private final DescriptorContext parent;
        private final FieldDescriptor fd;

        public FieldContext(DescriptorContext parent, FieldDescriptor fd) {
            this.parent = Objects.requireNonNull(parent);
            this.fd = Objects.requireNonNull(fd);
        }

        private ProtobufFunctions functions() {
            final ProtobufFunctions wellKnown = wellKnown().orElse(null);
            if (wellKnown != null) {
                return wellKnown;
            }
            if (fd.isRepeated()) {
                // todo and map
                return ProtobufFunctions.empty();
            }
            return new FieldObject(fd).functions();
        }

        private Optional<ProtobufFunctions> wellKnown() {
            // todo: eventually have support for parsing specific fields in specific ways
            return Optional.empty();
        }

        private ProtobufFunctions namedField(TypedFunction<Message> tf) {
            return ProtobufFunctions.builder()
                    .putColumns(List.of(fd.getName()), tf)
                    .build();
        }

        private DescriptorContext toMessageContext() {
            if (fd.getJavaType() != JavaType.MESSAGE) {
                throw new IllegalStateException();
            }
            final List<FieldDescriptor> parents = Stream.concat(parent.parents.stream(), Stream.of(fd))
                    .collect(Collectors.toList());
            return new DescriptorContext(parents, fd.getMessageType());
        }

        private class FieldObject implements ObjectFunction<Message, Object> {
            private final FieldDescriptor fd;

            public FieldObject(FieldDescriptor fd) {
                if (fd.isRepeated()) {
                    throw new IllegalArgumentException();
                }
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

            private ProtobufFunctions functions() {
                switch (fd.getJavaType()) {
                    case INT:
                        return namedField(mapInt(int_()));
                    case LONG:
                        return namedField(mapLong(long_()));
                    case FLOAT:
                        return namedField(mapFloat(float_()));
                    case DOUBLE:
                        return namedField(mapDouble(double_()));
                    case BOOLEAN:
                        return namedField(mapBoolean(boolean_()));
                    case STRING:
                        return namedField(as(Type.stringType()));
                    case BYTE_STRING:
                        return namedField(as(Type.ofCustom(ByteString.class)).mapObj(bytes_()));
                    case ENUM:
                        return namedField(as(Type.ofCustom(EnumValueDescriptor.class)));
                    case MESSAGE:
                        final DescriptorContext messageContext = toMessageContext();
                        final ProtobufFunctions subF = messageContext.functions();
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
