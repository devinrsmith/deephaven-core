package io.deephaven.client.impl;

import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.BooleanArray;
import io.deephaven.qst.array.ByteArray;
import io.deephaven.qst.array.CharArray;
import io.deephaven.qst.array.DoubleArray;
import io.deephaven.qst.array.FloatArray;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.array.LongArray;
import io.deephaven.qst.array.PrimitiveArray;
import io.deephaven.qst.array.ShortArray;
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
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.SmallIntWriter;

import java.util.Collection;
import java.util.Objects;

final class Writer {

    static void write(FieldWriter writer, Array<?> array) {
        array.walk(new ArrayWriter(writer));
    }

    static void write(FieldWriter writer, PrimitiveArray<?> array) {
        array.walk(new PrimitiveArrayWriter(writer));
    }

    static void write(FieldWriter writer, GenericArray<?> array) {
        array.componentType().walk(new GenericArrayWriter(writer, array));
    }

    static void write(SmallIntWriter writer, short[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            // todo: use writeNull?
            writer.writeSmallInt(array[offset + i]);
        }
    }

    static void write(IntWriter writer, int[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            // todo: use writeNull?
            writer.writeInt(array[offset + i]);
        }
    }

    static void write(BigIntWriter writer, long[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            // todo: use writeNull?
            writer.writeBigInt(array[offset + i]);
        }
    }

    static void write(Float4Writer writer, float[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            // todo: use writeNull?
            writer.writeFloat4(array[offset + i]);
        }
    }

    static void write(Float8Writer writer, double[] array, int offset, int len) {
        for (int i = 0; i < len; i++) {
            // todo: use writeNull?
            writer.writeFloat8(array[offset + i]);
        }
    }

    static void write(Float4Writer writer, Collection<Float> values) {
        for (Float value : values) {
            if (value == null) {
                writer.writeNull();
            } else {
                writer.writeFloat4(value);
            }
        }
    }

    static void write(Float8Writer writer, Collection<Double> values) {
        for (Double value : values) {
            if (value == null) {
                writer.writeNull();
            } else {
                writer.writeFloat8(value);
            }
        }
    }

    static class ArrayWriter implements Array.Visitor<Void> {
        private final FieldWriter writer;

        public ArrayWriter(FieldWriter writer) {
            this.writer = Objects.requireNonNull(writer);
        }

        @Override
        public Void visit(PrimitiveArray<?> primitive) {
            write(writer, primitive);
            return null;
        }

        @Override
        public Void visit(GenericArray<?> generic) {
            write(writer, generic);
            return null;
        }
    }

    static class PrimitiveArrayWriter implements PrimitiveArray.Visitor<Void> {
        private final FieldWriter writer;

        PrimitiveArrayWriter(FieldWriter writer) {
            this.writer = Objects.requireNonNull(writer);
        }

        @Override
        public Void visit(ByteArray byteArray) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void visit(BooleanArray booleanArray) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void visit(CharArray charArray) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void visit(ShortArray shortArray) {
            write(writer, shortArray.values(), 0, shortArray.size());
            return null;
        }

        @Override
        public Void visit(IntArray intArray) {
            write(writer, intArray.values(), 0, intArray.size());
            return null;
        }

        @Override
        public Void visit(LongArray longArray) {
            write(writer, longArray.values(), 0, longArray.size());
            return null;
        }

        @Override
        public Void visit(FloatArray floatArray) {
            write(writer, floatArray.values(), 0, floatArray.size());
            return null;
        }

        @Override
        public Void visit(DoubleArray doubleArray) {
            write(writer, doubleArray.values(), 0, doubleArray.size());
            return null;
        }
    }

    static class GenericArrayWriter implements GenericType.Visitor<Void>, BoxedType.Visitor<Void>, ArrayType.Visitor<Void> {
        private final FieldWriter writer;
        private final GenericArray<?> array;

        public GenericArrayWriter(FieldWriter writer, GenericArray<?> array) {
            this.writer = Objects.requireNonNull(writer);
            this.array = Objects.requireNonNull(array);
        }

        @Override
        public Void visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<Void>) this);
        }

        @Override
        public Void visit(ArrayType<?, ?> arrayType) {
            return arrayType.walk((ArrayType.Visitor<Void>) this);
        }

        @Override
        public Void visit(StringType stringType) {
            // todo
            // final GenericArray<String> array = arr.cast(stringType);
            throw new UnsupportedOperationException();
        }

        @Override
        public Void visit(InstantType instantType) {
            // todo
            // final GenericArray<Instant> array = generic.cast(instantType);
            throw new UnsupportedOperationException();
        }

        @Override
        public Void visit(CustomType<?> customType) {
            // todo: biginteger, bigdecimal, etc
            throw new UnsupportedOperationException();
        }

        @Override
        public Void visit(BoxedBooleanType booleanType) {
            return null;
        }

        @Override
        public Void visit(BoxedByteType byteType) {
            return null;
        }

        @Override
        public Void visit(BoxedCharType charType) {
            return null;
        }

        @Override
        public Void visit(BoxedShortType shortType) {
            return null;
        }

        @Override
        public Void visit(BoxedIntType intType) {
            return null;
        }

        @Override
        public Void visit(BoxedLongType longType) {
            return null;
        }

        @Override
        public Void visit(BoxedFloatType floatType) {
            write(writer, array.cast(floatType).values());
            return null;
        }

        @Override
        public Void visit(BoxedDoubleType doubleType) {
            write(writer, array.cast(doubleType).values());
            return null;
        }

        // -- nested stuff --


        @Override
        public Void visit(NativeArrayType<?, ?> nativeArrayType) {
            final ListWriter innerList = writer.list();
            innerList.startList();


            innerList.endList();
            return null;
        }

        @Override
        public Void visit(PrimitiveVectorType<?, ?> vectorPrimitiveType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void visit(GenericVectorType<?, ?> genericVectorType) {
            throw new UnsupportedOperationException();
        }
    }

    static class NestedArrayVisitor implements Type.Visitor<Void>, PrimitiveType.Visitor<Void>, GenericType.Visitor<Void> {

        private final FieldWriter writer;
        private final GenericArray<?> array;
        private final NativeArrayType<?, ?> inner;

        @Override
        public Void visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Void>) this);
        }

        @Override
        public Void visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Void>) this);
        }

        @Override
        public Void visit(BooleanType booleanType) {

            final GenericArray<boolean[]> cast = array.cast(booleanType.arrayType());

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
            return null;
        }

        @Override
        public Void visit(IntType intType) {
            return null;
        }

        @Override
        public Void visit(LongType longType) {
            return null;
        }

        @Override
        public Void visit(FloatType floatType) {
            return null;
        }

        @Override
        public Void visit(DoubleType doubleType) {
            return null;
        }
    }
}
