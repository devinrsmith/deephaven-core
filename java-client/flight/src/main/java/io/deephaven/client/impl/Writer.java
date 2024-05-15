package io.deephaven.client.impl;

import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.GenericVectorType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.SmallIntWriter;

import java.lang.reflect.Array;
import java.util.Objects;
import java.util.function.Consumer;

final class Writer {

    static <T> void writes(ListWriter writer, T array, int offset, int length, NativeArrayType<T, ?> type, boolean isTopLevel) {
        ArrayValues.writes(isTopLevel ? writer : writer.list(), array, offset, length, type);
    }

    static <CT> void writes(ListWriter writer, CT[] array, int offset, int len, NativeArrayType<CT[], CT> type, boolean isTopLevel) {
        final GenericType<CT> componentType = NativeArrayType.genericComponentType(type);
        final Consumer<CT> consumer = GenericValue.writer(isTopLevel ? writer : writer.list(), componentType);
        for (int i = 0; i < len; ++i) {
//            writer.setPosition(i);
            consumer.accept(array[offset + i]);
        }
    }

    static <CT> void writes(ListWriter writer, Iterable<CT> values, GenericType<CT> componentType, boolean isTopLevel) {
        int i = 0;
        final Consumer<CT> consumer = GenericValue.writer(isTopLevel ? writer : writer.list(), componentType);
        for (CT element : values) {
//            writer.setPosition(i);
            consumer.accept(element);
            ++i;
        }
    }

    static void writes(SmallIntWriter writer, short[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            writer.writeSmallInt(array[offset + i]);
        }
    }

    static void writes(IntWriter writer, int[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            writer.writeInt(array[offset + i]);
        }
    }

    static void writes(BigIntWriter writer, long[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            writer.writeBigInt(array[offset + i]);
        }
    }

    static void writes(Float4Writer writer, float[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            writer.writeFloat4(array[offset + i]);
        }
    }

    static void writes(Float8Writer writer, double[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            writer.writeFloat8(array[offset + i]);
        }
    }

    static class GenericValue<T> implements GenericType.Visitor<Consumer<T>>, BoxedType.Visitor<Consumer<T>>, ArrayType.Visitor<Consumer<T>> {

        public static <T> Consumer<T> writer(ListWriter writer, GenericType<T> type) {
            return type.walk(new GenericValue<>(writer));
        }

        private final ListWriter writer;

        private GenericValue(ListWriter writer) {
            this.writer = Objects.requireNonNull(writer);
        }

        private <Z> Consumer<T> consumer(@SuppressWarnings("unused") GenericType<Z> type, Consumer<Z> consumer) {
            //noinspection unchecked
            return (Consumer<T>) consumer;
        }

        @Override
        public Consumer<T> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<Consumer<T>>) this);
        }

        @Override
        public Consumer<T> visit(ArrayType<?, ?> arrayType) {
            return arrayType.walk((ArrayType.Visitor<Consumer<T>>) this);
        }

        @Override
        public Consumer<T> visit(StringType stringType) {
            return null;
        }

        @Override
        public Consumer<T> visit(InstantType instantType) {
            return null;
        }

        @Override
        public Consumer<T> visit(CustomType<?> customType) {
            return null;
        }

        @Override
        public Consumer<T> visit(BoxedBooleanType booleanType) {
            return null;
        }

        @Override
        public Consumer<T> visit(BoxedByteType byteType) {
            // todo
            return null;
        }

        @Override
        public Consumer<T> visit(BoxedCharType charType) {
            return null;
        }

        @Override
        public Consumer<T> visit(BoxedShortType shortType) {
            final SmallIntWriter smallIntWriter = writer.smallInt();
            return consumer(shortType, x -> write(smallIntWriter, x));
        }

        private static void write(SmallIntWriter writer, Short value) {
            if (value == null) {
                writer.writeNull();
            } else {
                writer.writeSmallInt(value);
            }
        }

        @Override
        public Consumer<T> visit(BoxedIntType intType) {
            final IntWriter intWriter = writer.integer();
            return consumer(intType, x -> write(intWriter, x));
        }

        private static void write(IntWriter writer, Integer value) {
            if (value == null) {
                writer.writeNull();
            } else {
                writer.writeInt(value);
            }
        }

        @Override
        public Consumer<T> visit(BoxedLongType longType) {
            final BigIntWriter bigIntWriter = writer.bigInt();
            return consumer(longType, x -> write(bigIntWriter, x));
        }

        private static void write(BigIntWriter writer, Long value) {
            if (value == null) {
                writer.writeNull();
            } else {
                writer.writeBigInt(value);
            }
        }

        @Override
        public Consumer<T> visit(BoxedFloatType floatType) {
            final Float4Writer float4Writer = writer.float4();
            return consumer(floatType, x -> write(float4Writer, x));
        }

        private static void write(Float4Writer writer, Float value) {
            if (value == null) {
                writer.writeNull();
            } else {
                writer.writeFloat4(value);
            }
        }

        @Override
        public Consumer<T> visit(BoxedDoubleType doubleType) {
            final Float8Writer float8Writer = writer.float8();
            return consumer(doubleType, x -> write(float8Writer, x));
        }

        private static void write(Float8Writer writer, Double value) {
            if (value == null) {
                writer.writeNull();
            } else {
                writer.writeFloat8(value);
            }
        }

        // nested

        @Override
        public Consumer<T> visit(NativeArrayType<?, ?> nativeArrayType) {
            return listWriter(nativeArrayType);
        }

        private <AT> Consumer<T> listWriter(NativeArrayType<AT, ?> type) {
            // we only apply .list() if it's _not_ the top-level
//            final ListWriter listWriter = writer instanceof UnionListWriter ? writer : writer.list();
            return consumer(type, array -> write(writer, type, array));
        }

        private static <AT> void write(ListWriter listWriter, NativeArrayType<AT, ?> type, AT value) {
            if (value == null) {
                listWriter.writeNull();
            } else {
                listWriter.startList();
                final int L = Array.getLength(value);
                Writer.writes(listWriter, value, 0, L, type, false);
                listWriter.endList();
            }
        }

        @Override
        public Consumer<T> visit(PrimitiveVectorType<?, ?> vectorPrimitiveType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Consumer<T> visit(GenericVectorType<?, ?> genericVectorType) {
            throw new UnsupportedOperationException();
        }
    }

    static class ArrayValues implements Type.Visitor<Void>, PrimitiveType.Visitor<Void> {

        public static <T> void writes(ListWriter writer, T array, int offset, int length, NativeArrayType<T, ?> type) {
            type.componentType().walk(new ArrayValues(writer, array, offset, length));
        }

        private final ListWriter writer;
        private final Object array;
        private final int offset;
        private final int length;

        private ArrayValues(ListWriter writer, Object array, int offset, int length) {
            this.writer = Objects.requireNonNull(writer);
            this.array = Objects.requireNonNull(array);
            this.offset = offset;
            this.length = length;
        }

        private <T> T array(NativeArrayType<T, ?> type) {
            return type.clazz().cast(array);
        }

        private <ComponentType> void writesGeneric(GenericType<ComponentType> componentType) {
            final NativeArrayType<ComponentType[], ComponentType> arrayType = componentType.arrayType();
            Writer.writes(writer, array(arrayType), offset, length, arrayType, false);
        }

        @Override
        public Void visit(GenericType<?> genericType) {
            writesGeneric(genericType);
            return null;
        }

        @Override
        public Void visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Void>) this);
        }

        @Override
        public Void visit(BooleanType booleanType) {
            return null;
        }

        @Override
        public Void visit(ByteType byteType) {
            return null;
        }

        @Override
        public Void visit(CharType charType) {
            return null;
        }

        @Override
        public Void visit(ShortType shortType) {
            Writer.writes(writer.smallInt(), array(shortType.arrayType()), offset, length);
            return null;
        }

        @Override
        public Void visit(IntType intType) {
            Writer.writes(writer.integer(), array(intType.arrayType()), offset, length);
            return null;
        }

        @Override
        public Void visit(LongType longType) {
            Writer.writes(writer.bigInt(), array(longType.arrayType()), offset, length);
            return null;
        }

        @Override
        public Void visit(FloatType floatType) {
            Writer.writes(writer.float4(), array(floatType.arrayType()), offset, length);
            return null;
        }

        @Override
        public Void visit(DoubleType doubleType) {
            Writer.writes(writer.float8(), array(doubleType.arrayType()), offset, length);
            return null;
        }
    }
}
