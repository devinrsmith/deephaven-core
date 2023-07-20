package io.deephaven.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.deephaven.protobuf.ProtobufFunctions.Builder;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.PrimitiveFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.stream.blink.tf.TypedFunction.Visitor;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class Protobuf {
    private static final ObjectFunction<Object, ByteString> BYTE_STRING_OBJ =
            ObjectFunction.cast(Type.ofCustom(ByteString.class));
    private static final ObjectFunction<Object, EnumValueDescriptor> ENUM_VALUE_DESCRIPTOR_OBJ =
            ObjectFunction.cast(Type.ofCustom(EnumValueDescriptor.class));
    private static final CustomType<Message> MESSAGE_TYPE = Type.ofCustom(Message.class);

    private static final ObjectFunction<Object, Message> MESSAGE_OBJ = ObjectFunction.cast(MESSAGE_TYPE);

    private static final IntFunction<Message> SERIALIZED_SIZE_FUNCTION = Message::getSerializedSize;
    private static final ObjectFunction<Message, UnknownFieldSet> UNKNOWN_FIELD_SET_FUNCTION =
            ObjectFunction.of(Message::getUnknownFields, Type.ofCustom(UnknownFieldSet.class));
    private static final ObjectFunction<Message, Message> MESSAGE_IDENTITY_FUNCTION =
            ObjectFunction.identity(MESSAGE_TYPE);

    private static final ObjectFunction<ByteString, byte[]> BYTE_STRING_FUNCTION =
            ObjectFunction.of(ByteString::toByteArray, Type.byteType().arrayType()).onNullInput(null);
    private static final ObjectFunction<Object, String> STRING_OBJ = ObjectFunction.cast(Type.stringType());
    private static final ObjectFunction<Object, Integer> BOXED_INT_OBJ = ObjectFunction.cast(BoxedIntType.of());
    private static final ObjectFunction<Object, Long> BOXED_LONG_OBJ = ObjectFunction.cast(BoxedLongType.of());
    private static final ObjectFunction<Object, Float> BOXED_FLOAT_OBJ = ObjectFunction.cast(BoxedFloatType.of());
    private static final ObjectFunction<Object, Double> BOXED_DOUBLE_OBJ = ObjectFunction.cast(BoxedDoubleType.of());
    private static final ObjectFunction<Object, Boolean> BOXED_BOOLEAN_OBJ = ObjectFunction.cast(BoxedBooleanType.of());

    private final ProtobufOptions options;

    public Protobuf(ProtobufOptions options) {
        this.options = Objects.requireNonNull(options);
    }

    // Note: in protobuf an actualized Message is never null.
    // In the case of our translation layer though, the presence of a null Message means that the field, or some parent
    // of the field, was not present; but we still need to translate the value for the specific column.
    static boolean hasField(Message m, FieldDescriptor fd) {
        return m != null && (m.hasField(fd) || !fd.hasPresence());
    }

    public ProtobufFunctions translate(Descriptor descriptor) {
        return new DescriptorContext(List.of(), descriptor).functions();
    }

    private static List<String> prefix(String name, List<String> rest) {
        return Stream.concat(Stream.of(name), rest.stream()).collect(Collectors.toList());
    }

    private TypedFunction<Message> parser(SingleValuedMessageParser svmp) {
        return svmp.parser(options);
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
            options.serializedSizeName().ifPresent(x -> builder.putColumns(List.of(x), SERIALIZED_SIZE_FUNCTION));
            options.unknownFieldSetName().ifPresent(x -> builder.putColumns(List.of(x), UNKNOWN_FIELD_SET_FUNCTION));
            options.rawMessageName().ifPresent(x -> builder.putColumns(List.of(x), MESSAGE_IDENTITY_FUNCTION));
            return builder.build();
        }

        private Optional<ProtobufFunctions> wellKnown() {
            // todo: eventually support cases that are >1 field
            return Optional.ofNullable(options.parsers().get(descriptor.getFullName()))
                    .map(Protobuf.this::parser)
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
            if (!options.include(fullPath())) {
                return ProtobufFunctions.empty();
            }
            final ProtobufFunctions wellKnown = wellKnown().orElse(null);
            if (wellKnown != null) {
                return wellKnown;
            }
            if (fd.isMapField()) {
                return new MapFieldObject().functions();
            }
            if (fd.isRepeated()) {
                return new RepeatedFieldObject().functions();
            }
            return new FieldObject().functions();
        }

        private List<String> fullPath() {
            return Stream.concat(parent.parents.stream(), Stream.of(fd))
                    .map(FieldDescriptor::getName)
                    .collect(Collectors.toList());
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

            @Override
            public GenericType<Object> returnType() {
                return Type.ofCustom(Object.class);
            }

            @Override
            public Object apply(Message value) {
                return hasField(value, fd) ? value.getField(fd) : null;
            }

            private ProtobufFunctions functions() {
                switch (fd.getJavaType()) {
                    case INT:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_INT_OBJ))
                                : namedField(mapInt(IntFunction.primitive()));
                    case LONG:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_LONG_OBJ))
                                : namedField(mapLong(LongFunction.primitive()));
                    case FLOAT:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_FLOAT_OBJ))
                                : namedField(mapFloat(FloatFunction.primitive()));
                    case DOUBLE:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_DOUBLE_OBJ))
                                : namedField(mapDouble(DoubleFunction.primitive()));
                    case BOOLEAN:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_BOOLEAN_OBJ))
                                : namedField(mapBoolean(BooleanFunction.primitive()));
                    case STRING:
                        return namedField(mapObj(STRING_OBJ));
                    case BYTE_STRING:
                        return namedField(mapObj(BYTE_STRING_OBJ).mapObj(BYTE_STRING_FUNCTION));
                    case ENUM:
                        return namedField(mapObj(ENUM_VALUE_DESCRIPTOR_OBJ));
                    case MESSAGE:
                        final Function<Message, Message> fieldAsMessage = mapObj(MESSAGE_OBJ)::apply;
                        final DescriptorContext messageContext = toMessageContext();
                        final ProtobufFunctions subF = messageContext.functions();
                        final Builder builder = ProtobufFunctions.builder();

                        final boolean parentFieldIsRepeated = !parent.parents.isEmpty()
                                && parent.parents.get(parent.parents.size() - 1).isRepeated();

                        for (Entry<List<String>, TypedFunction<Message>> e : subF.columns().entrySet()) {
                            final List<String> key = e.getKey();
                            // The majority of the time, we need to BypassOnNull b/c the Message may be null. In the
                            // case where the message is part of a repeated field though, the Message is never null
                            // (it's always implicitly present based on the repeated count).
                            final TypedFunction<Message> value = parentFieldIsRepeated
                                    ? e.getValue()
                                    : BypassOnNull.of(e.getValue());
                            builder.putColumns(prefix(fd.getName(), key), value.mapInput(fieldAsMessage));
                        }
                        return builder.build();
                    default:
                        throw new IllegalStateException();
                }
            }
        }

        private class MapFieldObject {

            private ProtobufFunctions functions() {
                // https://protobuf.dev/programming-guides/proto3/#maps
                // For maps fields:
                // map<KeyType, ValueType> map_field = 1;
                // The parsed descriptor looks like:
                // message MapFieldEntry {
                // option map_entry = true;
                // optional KeyType key = 1;
                // optional ValueType value = 2;
                // }
                // repeated MapFieldEntry map_field = 1;

                if (fd.getMessageType().getFields().size() != 2) {
                    throw new IllegalStateException("Expected map to have exactly 2 field descriptors");
                }
                final FieldDescriptor keyFd = fd.getMessageType().findFieldByNumber(1);
                if (keyFd == null) {
                    throw new IllegalStateException("Expected map to have field descriptor number 1 (key)");
                }
                final FieldDescriptor valueFd = fd.getMessageType().findFieldByNumber(2);
                if (valueFd == null) {
                    throw new IllegalStateException("Expected map to have field descriptor number 2 (value)");
                }

                final List<FieldDescriptor> parents = Stream.concat(parent.parents.stream(), Stream.of(fd))
                        .collect(Collectors.toList());
                final DescriptorContext dc = new DescriptorContext(parents, fd.getMessageType());
                final ProtobufFunctions keyFunctions = new FieldContext(dc, keyFd).functions();
                if (keyFunctions.columns().size() != 1) {
                    throw new IllegalStateException("Expected map key to be single type");
                }

                final ProtobufFunctions valueFunctions = new FieldContext(dc, valueFd).functions();
                if (valueFunctions.columns().size() != 1) {
                    // We've parsed the value type as an entity that has multiple values (as opposed to a single value
                    // we can put into a map). We may wish to have more configuration options for these situations in
                    // the future (ie, throw an exception or something else). For now, we're going to treat this case as
                    // "simple" repeated type.
                    return delegate();
                }

                final TypedFunction<Message> keyFunction = keyFunctions.columns().values().iterator().next();
                final TypedFunction<Message> valueFunction = valueFunctions.columns().values().iterator().next();
                return namedField(ObjectFunction.of(message -> {
                    final Map<Object, Object> map = new HashMap<>();
                    final int count = message.getRepeatedFieldCount(fd);
                    for (int i = 0; i < count; ++i) {
                        final Message obj = (Message) message.getRepeatedField(fd, i);
                        final Object key = UpcastApply.apply(keyFunction, obj);
                        final Object value = UpcastApply.apply(valueFunction, obj);
                        map.put(key, value);
                    }
                    return map;
                }, Type.ofCustom(Map.class)));
            }

            private ProtobufFunctions delegate() {
                return new RepeatedFieldObject().functions();
            }
        }

        private class RepeatedFieldObject {

            private ProtobufFunctions functions() {
                switch (fd.getJavaType()) {
                    case INT:
                        return namedField(mapInts(IntFunction.primitive()));
                    case LONG:
                        return namedField(mapLongs(LongFunction.primitive()));
                    case FLOAT:
                        return namedField(mapFloats(FloatFunction.primitive()));
                    case DOUBLE:
                        return namedField(mapDoubles(DoubleFunction.primitive()));
                    case BOOLEAN:
                        return namedField(mapBooleans(BooleanFunction.primitive()));
                    case STRING:
                        return namedField(mapGenerics(STRING_OBJ));
                    case BYTE_STRING:
                        return namedField(mapGenerics(BYTE_STRING_OBJ.mapObj(BYTE_STRING_FUNCTION)));
                    case ENUM:
                        return namedField(mapGenerics(ENUM_VALUE_DESCRIPTOR_OBJ));
                    case MESSAGE:
                        final DescriptorContext messageContext = toMessageContext();
                        final ProtobufFunctions functions = messageContext.functions();
                        final Builder builder = ProtobufFunctions.builder();
                        for (Entry<List<String>, TypedFunction<Message>> e : functions.columns().entrySet()) {
                            final List<String> path = e.getKey();
                            final TypedFunction<Message> tf = e.getValue();
                            final ObjectFunction<Message, ?> repeatedTf = tf.walk(new ToRepeatedType());
                            builder.putColumns(prefix(fd.getName(), path), repeatedTf);
                        }
                        return builder.build();
                    default:
                        throw new IllegalStateException();
                }
            }

            private ObjectFunction<Message, char[]> mapChars(CharFunction<Object> f) {
                return ObjectFunction.of(m -> toChars(m, f), Type.charType().arrayType());
            }

            private ObjectFunction<Message, byte[]> mapBytes(ByteFunction<Object> f) {
                return ObjectFunction.of(m -> toBytes(m, f), Type.byteType().arrayType());
            }

            private ObjectFunction<Message, short[]> mapShorts(ShortFunction<Object> f) {
                return ObjectFunction.of(m -> toShorts(m, f), Type.shortType().arrayType());
            }

            private ObjectFunction<Message, int[]> mapInts(IntFunction<Object> f) {
                return ObjectFunction.of(m -> toInts(m, f), Type.intType().arrayType());
            }

            private ObjectFunction<Message, long[]> mapLongs(LongFunction<Object> f) {
                return ObjectFunction.of(m -> toLongs(m, f), Type.longType().arrayType());
            }

            private ObjectFunction<Message, float[]> mapFloats(FloatFunction<Object> f) {
                return ObjectFunction.of(m -> toFloats(m, f), Type.floatType().arrayType());
            }

            private ObjectFunction<Message, double[]> mapDoubles(DoubleFunction<Object> f) {
                return ObjectFunction.of(m -> toDoubles(m, f), Type.doubleType().arrayType());
            }

            private ObjectFunction<Message, boolean[]> mapBooleans(BooleanFunction<Object> f) {
                return ObjectFunction.of(m -> toBooleans(m, f), Type.booleanType().arrayType());
            }

            private <T> ObjectFunction<Message, T[]> mapGenerics(ObjectFunction<Object, T> f) {
                return ObjectFunction.of(message -> toArray(message, f), f.returnType().arrayType());
            }

            private char[] toChars(Message message, CharFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final char[] array = new char[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsChar(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private byte[] toBytes(Message message, ByteFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final byte[] array = new byte[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsByte(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private short[] toShorts(Message message, ShortFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final short[] array = new short[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsShort(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private int[] toInts(Message message, IntFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final int[] array = new int[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsInt(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private long[] toLongs(Message message, LongFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final long[] array = new long[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsLong(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private float[] toFloats(Message message, FloatFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final float[] array = new float[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsFloat(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private double[] toDoubles(Message message, DoubleFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final double[] array = new double[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsDouble(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private boolean[] toBooleans(Message message, BooleanFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final boolean[] array = new boolean[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsBoolean(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private <T> T[] toArray(Message message, ObjectFunction<Object, T> f) {
                final int count = message.getRepeatedFieldCount(fd);
                // noinspection unchecked
                final T[] array = (T[]) Array.newInstance(f.returnType().clazz(), count);
                for (int i = 0; i < count; ++i) {
                    array[i] = f.apply(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private class ToRepeatedType implements
                    Visitor<Message, ObjectFunction<Message, ?>>,
                    PrimitiveFunction.Visitor<Message, ObjectFunction<Message, ?>> {

                @Override
                public ObjectFunction<Message, ?> visit(ObjectFunction<Message, ?> f) {
                    return mapGenerics(MESSAGE_OBJ.mapObj(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(PrimitiveFunction<Message> f) {
                    return f.walk((PrimitiveFunction.Visitor<Message, ObjectFunction<Message, ?>>) this);
                }

                @Override
                public ObjectFunction<Message, ?> visit(BooleanFunction<Message> f) {
                    return mapBooleans(MESSAGE_OBJ.mapBoolean(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(CharFunction<Message> f) {
                    return mapChars(MESSAGE_OBJ.mapChar(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(ByteFunction<Message> f) {
                    return mapBytes(MESSAGE_OBJ.mapByte(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(ShortFunction<Message> f) {
                    return mapShorts(MESSAGE_OBJ.mapShort(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(IntFunction<Message> f) {
                    return mapInts(MESSAGE_OBJ.mapInt(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(LongFunction<Message> f) {
                    return mapLongs(MESSAGE_OBJ.mapLong(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(FloatFunction<Message> f) {
                    return mapFloats(MESSAGE_OBJ.mapFloat(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(DoubleFunction<Message> f) {
                    return mapDoubles(MESSAGE_OBJ.mapDouble(f));
                }
            }

            private ProtobufFunctions namedField(TypedFunction<Message> tf) {
                return ProtobufFunctions.builder()
                        .putColumns(List.of(fd.getName()), tf)
                        .build();
            }
        }
    }
}
