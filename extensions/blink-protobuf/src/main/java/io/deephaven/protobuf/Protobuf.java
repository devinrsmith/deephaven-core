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
import io.deephaven.stream.blink.tf.ApplyVisitor;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.NullGuard;
import io.deephaven.stream.blink.tf.ObjectFunction;
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

    interface Unmapped {
    }

    private static final CustomType<ByteString> BYTE_STRING_TYPE = Type.ofCustom(ByteString.class);
    private static final CustomType<EnumValueDescriptor> ENUM_VALUE_DESCRIPTOR_TYPE =
            Type.ofCustom(EnumValueDescriptor.class);
    private static final CustomType<Message> MESSAGE_TYPE = Type.ofCustom(Message.class);

    private static final CustomType<UnknownFieldSet> UNKNOWN_FIELD_SET_TYPE = Type.ofCustom(UnknownFieldSet.class);
    private static final ObjectFunction<Object, Message> CAST_MESSAGE_TYPE = ObjectFunction.cast(MESSAGE_TYPE);

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
        // return NullGuard.of(svmp.parser(options));
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
            options.serializedSizeName().ifPresent(x -> builder.putColumns(List.of(x), serializedSize()));
            options.unknownFieldSetName().ifPresent(x -> builder.putColumns(List.of(x), unknownFieldSet()));
            options.rawMessageName().ifPresent(x -> builder.putColumns(List.of(x), messageIdentity()));
            return builder.build();
        }

        private Optional<ProtobufFunctions> wellKnown() {
            // todo: eventually support cases that are >1 field
            return wellKnownSingular()
                    .map(ProtobufFunctions::unnamed);
        }

        private Optional<TypedFunction<Message>> wellKnownSingular() {
            return svmp()
                    .map(Protobuf.this::parser);
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

    private static ObjectFunction<Message, Message> messageIdentity() {
        return ObjectFunction.identity(MESSAGE_TYPE);
    }

    private static ObjectFunction<Message, UnknownFieldSet> unknownFieldSet() {
        return ObjectFunction.of(Message::getUnknownFields, UNKNOWN_FIELD_SET_TYPE);
    }

    private static IntFunction<Message> serializedSize() {
        return Message::getSerializedSize;
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
                        return namedField(as(BoxedIntType.of()));
                    case LONG:
                        return namedField(as(BoxedLongType.of()));
                    case FLOAT:
                        return namedField(as(BoxedFloatType.of()));
                    case DOUBLE:
                        return namedField(as(BoxedDoubleType.of()));
                    case BOOLEAN:
                        return namedField(as(BoxedBooleanType.of()));
                    case STRING:
                        return namedField(as(Type.stringType()));
                    case BYTE_STRING:
                        return namedField(as(BYTE_STRING_TYPE).mapObj(bytes_()));
                    case ENUM:
                        return namedField(as(ENUM_VALUE_DESCRIPTOR_TYPE));
                    case MESSAGE:
                        final Function<Message, Message> messageF = as(MESSAGE_TYPE)::apply;
                        final DescriptorContext messageContext = toMessageContext();
                        final ProtobufFunctions subF = messageContext.functions();
                        final Builder builder = ProtobufFunctions.builder();
                        for (Entry<List<String>, TypedFunction<Message>> e : subF.columns().entrySet()) {
                            final List<String> key = e.getKey();
                            final TypedFunction<Message> value = OnNullWrapper.of(e.getValue());
                            builder.putColumns(prefix(fd.getName(), key), value.mapInput(messageF));
                        }
                        return builder.build();
                    default:
                        throw new IllegalStateException();
                }
            }
        }

        private class MapFieldObject {

            private ProtobufFunctions functions() {
                // For maps fields:
                // map<KeyType, ValueType> map_field = 1;
                // The parsed descriptor looks like:
                // message MapFieldEntry {
                // option map_entry = true;
                // optional KeyType key = 1;
                // optional ValueType value = 2;
                // }
                // repeated MapFieldEntry map_field = 1;
                final FieldDescriptor keyFd = fd.getMessageType().findFieldByNumber(1);
                if (keyFd == null) {
                    return delegate();
                }

                final FieldDescriptor valueFd = fd.getMessageType().findFieldByNumber(2);
                if (valueFd == null) {
                    return delegate();
                }

                final List<FieldDescriptor> parents = Stream.concat(parent.parents.stream(), Stream.of(fd))
                        .collect(Collectors.toList());
                final DescriptorContext dc = new DescriptorContext(parents, fd.getMessageType());
                final ProtobufFunctions keyFunctions = new FieldContext(dc, keyFd).functions();
                if (keyFunctions.columns().size() != 1) {
                    return delegate();
                }

                final ProtobufFunctions valueFunctions = new FieldContext(dc, valueFd).functions();
                if (valueFunctions.columns().size() != 1) {
                    return delegate();
                }

                final TypedFunction<Object> keyTf =
                        CAST_MESSAGE_TYPE.map(keyFunctions.columns().values().iterator().next());
                final TypedFunction<Object> valueTf =
                        CAST_MESSAGE_TYPE.map(valueFunctions.columns().values().iterator().next());

                return namedField(ObjectFunction.of(message -> {
                    final Map<Object, Object> map = new HashMap<>();
                    final int count = message.getRepeatedFieldCount(fd);
                    for (int i = 0; i < count; ++i) {
                        final Object obj = message.getRepeatedField(fd, i);
                        final Object key = keyTf.walk(new ApplyVisitor<>(obj));
                        final Object value = valueTf.walk(new ApplyVisitor<>(obj));
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
                        return namedField(asInts());
                    case LONG:
                        return namedField(asLongs());
                    case FLOAT:
                        return namedField(asFloats());
                    case DOUBLE:
                        return namedField(asDoubles());
                    case BOOLEAN:
                        return namedField(asBooleans());
                    case STRING:
                        return namedField(asStrings());
                    case BYTE_STRING:
                        return namedField(asBytesBytes());
                    case ENUM:
                        return namedField(asEnums());
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

            private class ToRepeatedType implements Visitor<Message, ObjectFunction<Message, ?>> {
                @Override
                public ObjectFunction<Message, ?> visit(BooleanFunction<Message> f) {
                    return asBooleans(f);
                }

                @Override
                public ObjectFunction<Message, ?> visit(CharFunction<Message> f) {
                    return asChars(f);
                }

                @Override
                public ObjectFunction<Message, ?> visit(ByteFunction<Message> f) {
                    return asBytes(f);
                }

                @Override
                public ObjectFunction<Message, ?> visit(ShortFunction<Message> f) {
                    return asShorts(f);
                }

                @Override
                public ObjectFunction<Message, ?> visit(IntFunction<Message> f) {
                    return asInts(f);
                }

                @Override
                public ObjectFunction<Message, ?> visit(LongFunction<Message> f) {
                    return asLongs(f);
                }

                @Override
                public ObjectFunction<Message, ?> visit(FloatFunction<Message> f) {
                    return asFloats(f);
                }

                @Override
                public ObjectFunction<Message, ?> visit(DoubleFunction<Message> f) {
                    return asDoubles(f);
                }

                @Override
                public ObjectFunction<Message, ?> visit(ObjectFunction<Message, ?> f) {
                    return asArray(f);
                }
            }

            private ObjectFunction<Message, int[]> asInts() {
                return ObjectFunction.of(this::toInts, Type.intType().arrayType());
            }

            private ObjectFunction<Message, int[]> asInts(IntFunction<Message> f) {
                return ObjectFunction.of(m -> toInts(m, CAST_MESSAGE_TYPE.mapInt(f)), Type.intType().arrayType());
            }

            private int[] toInts(Message m) {
                return toInts(m, IntFunction.primitive());
            }

            private int[] toInts(Message message, IntFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final int[] array = new int[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsInt(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private ObjectFunction<Message, long[]> asLongs() {
                return ObjectFunction.of(this::toLongs, Type.longType().arrayType());
            }

            private ObjectFunction<Message, long[]> asLongs(LongFunction<Message> f) {
                return ObjectFunction.of(m -> toLongs(m, CAST_MESSAGE_TYPE.mapLong(f)), Type.longType().arrayType());
            }

            private long[] toLongs(Message message) {
                return toLongs(message, LongFunction.primitive());
            }

            private long[] toLongs(Message message, LongFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final long[] array = new long[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsLong(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private ObjectFunction<Message, float[]> asFloats() {
                return ObjectFunction.of(this::toFloats, Type.floatType().arrayType());
            }

            private ObjectFunction<Message, float[]> asFloats(FloatFunction<Message> f) {
                return ObjectFunction.of(m -> toFloats(m, CAST_MESSAGE_TYPE.mapFloat(f)), Type.floatType().arrayType());
            }

            private float[] toFloats(Message message) {
                return toFloats(message, FloatFunction.primitive());
            }

            private float[] toFloats(Message message, FloatFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final float[] array = new float[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsFloat(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private ObjectFunction<Message, double[]> asDoubles() {
                return ObjectFunction.of(this::toDoubles, Type.doubleType().arrayType());
            }

            private ObjectFunction<Message, double[]> asDoubles(DoubleFunction<Message> f) {
                return ObjectFunction.of(m -> toDoubles(m, CAST_MESSAGE_TYPE.mapDouble(f)),
                        Type.doubleType().arrayType());
            }

            private double[] toDoubles(Message message) {
                return toDoubles(message, DoubleFunction.primitive());
            }

            private double[] toDoubles(Message message, DoubleFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final double[] array = new double[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsDouble(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private ObjectFunction<Message, boolean[]> asBooleans() {
                return ObjectFunction.of(this::toBooleans, Type.booleanType().arrayType());
            }

            private ObjectFunction<Message, boolean[]> asBooleans(BooleanFunction<Message> f) {
                return ObjectFunction.of(m -> toBooleans(m, CAST_MESSAGE_TYPE.mapBoolean(f)),
                        Type.booleanType().arrayType());
            }

            private boolean[] toBooleans(Message message) {
                return toBooleans(message, BooleanFunction.primitive());
            }

            private boolean[] toBooleans(Message message, BooleanFunction<Object> f) {
                final int count = message.getRepeatedFieldCount(fd);
                final boolean[] array = new boolean[count];
                for (int i = 0; i < count; ++i) {
                    array[i] = f.applyAsBoolean(message.getRepeatedField(fd, i));
                }
                return array;
            }

            private <T> ObjectFunction<Message, T[]> asArray(ObjectFunction<Message, T> f) {
                return ObjectFunction.of(message -> toArray(message, CAST_MESSAGE_TYPE.mapObj(f)),
                        f.returnType().arrayType());
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

            private ObjectFunction<Message, byte[][]> asBytesBytes() {
                return ObjectFunction.of(this::toBytesBytes, Type.byteType().arrayType().arrayType());
            }

            private byte[][] toBytesBytes(Message message) {
                final int count = message.getRepeatedFieldCount(fd);
                final byte[][] array = new byte[count][];
                for (int i = 0; i < count; ++i) {
                    array[i] = ((ByteString) message.getRepeatedField(fd, i)).toByteArray();
                }
                return array;
            }

            private ObjectFunction<Message, String[]> asStrings() {
                return asGenericTypes(Type.stringType());
            }

            private ObjectFunction<Message, EnumValueDescriptor[]> asEnums() {
                return asGenericTypes(ENUM_VALUE_DESCRIPTOR_TYPE);
            }

            private ObjectFunction<Message, Message[]> asMessages() {
                return asGenericTypes(MESSAGE_TYPE);
            }

            private <T> ObjectFunction<Message, T[]> asGenericTypes(GenericType<T> gt) {
                return ObjectFunction.of(message -> toArray(message, ObjectFunction.cast(gt)), gt.arrayType());
                // return ObjectFunction.of(message -> toArray(gt, message, gt.clazz()::cast), gt.arrayType());
            }

            // note: no plain asChars(), no native byte in protobuf
            private ObjectFunction<Message, char[]> asChars(CharFunction<Message> f) {
                return ObjectFunction.of(m -> toChars(m, CAST_MESSAGE_TYPE.mapChar(f)), Type.charType().arrayType());
            }

            // note: no plain asBytes(), no native byte in protobuf
            private ObjectFunction<Message, byte[]> asBytes(ByteFunction<Message> f) {
                return ObjectFunction.of(m -> toBytes(m, CAST_MESSAGE_TYPE.mapByte(f)), Type.byteType().arrayType());
            }

            // note: no plain asShorts(), no native short in protobuf
            private ObjectFunction<Message, short[]> asShorts(ShortFunction<Message> f) {
                return ObjectFunction.of(m -> toShorts(m, CAST_MESSAGE_TYPE.mapShort(f)), Type.shortType().arrayType());
            }

            // Does not work, improper types
            // private ObjectFunction<Message, Boolean[]> asBoxedBooleans(BoxedBooleanFunction<Message> f) {
            // return ObjectFunction.of(m -> toBoxedBooleans(m, CAST_MESSAGE_TYPE.mapBoolean(f)),
            // Type.booleanType().arrayType());
            // }

            private ObjectFunction<Message, Unmapped> asUnmapped() {
                return ObjectFunction.ofSingle(null, Type.ofCustom(Unmapped.class));
            }

            private ProtobufFunctions namedField(TypedFunction<Message> tf) {
                return ProtobufFunctions.builder()
                        .putColumns(List.of(fd.getName()), tf)
                        .build();
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
        }
    }

    // private static BooleanFunction<Object> boolean_() {
    // return NullGuard.of((BooleanFunction<Object>) x -> (boolean) x);
    // }

    private static ObjectFunction<ByteString, byte[]> bytes_() {
        return NullGuard.of(ObjectFunction.of(ByteString::toByteArray, Type.byteType().arrayType()));
    }
}
