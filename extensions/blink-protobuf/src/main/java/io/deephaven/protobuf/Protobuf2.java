package io.deephaven.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import io.deephaven.protobuf.ProtobufFunctions.Builder;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.BoxedBooleanFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.NullGuard;
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

    private static final CustomType<ByteString> BYTE_STRING_TYPE = Type.ofCustom(ByteString.class);
    private static final CustomType<EnumValueDescriptor> ENUM_VALUE_DESCRIPTOR_TYPE = Type.ofCustom(EnumValueDescriptor.class);

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
                        return namedField(asInt());
                    case LONG:
                        return namedField(asLong());
                    case FLOAT:
                        return namedField(asFloat());
                    case DOUBLE:
                        return namedField(asDouble());
                    case BOOLEAN:
                        return namedField(asBoolean());
                    case STRING:
                        return namedField(asString());
                    case BYTE_STRING:
                        return namedField(asBytes());
                    case ENUM:
                        return namedField(asEnum());
                    case MESSAGE:
                        final Function<Message, Message> messageF = asMessage()::apply;
                        final DescriptorContext messageContext = toMessageContext();
                        final ProtobufFunctions subF = messageContext.functions();
                        final Builder builder = ProtobufFunctions.builder();
                        for (Entry<List<String>, TypedFunction<Message>> e : subF.columns().entrySet()) {
                            final List<String> key = e.getKey();
                            final TypedFunction<Message> value = e.getValue();
                            builder.putColumns(prefix(fd.getName(), key), value.mapInput(messageF));
                        }
                        return builder.build();
                    default:
                        throw new IllegalStateException();
                }
            }

            private IntFunction<Message> asInt() {
                return mapInt(IntFunction.guardedPrimitive());
            }

            private LongFunction<Message> asLong() {
                return mapLong(LongFunction.guardedPrimitive());
            }

            private FloatFunction<Message> asFloat() {
                return mapFloat(FloatFunction.guardedPrimitive());
            }

            private DoubleFunction<Message> asDouble() {
                return mapDouble(DoubleFunction.guardedPrimitive());
            }

            private BoxedBooleanFunction<Message> asBoolean() {
                return mapBoolean(boolean_());
            }

            private ObjectFunction<Message, String> asString() {
                return as(Type.stringType());
            }

            private ObjectFunction<Message, byte[]> asBytes() {
                return as(BYTE_STRING_TYPE).mapObj(bytes_());
            }

            private ObjectFunction<Message, EnumValueDescriptor> asEnum() {
                return as(ENUM_VALUE_DESCRIPTOR_TYPE);
            }

            private ObjectFunction<Message, Message> asMessage() {
                return as(Type.ofCustom(Message.class));
            }
        }
    }

    private static BoxedBooleanFunction<Object> boolean_() {
        return NullGuard.of((BoxedBooleanFunction<Object>) x -> (boolean) x);
    }

    private static ObjectFunction<ByteString, byte[]> bytes_() {
        return NullGuard.of(ObjectFunction.of(ByteString::toByteArray, Type.byteType().arrayType()));
    }
}
