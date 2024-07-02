package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.GenericVectorType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;

import java.util.function.Function;

public final class ArrayCopyPermuteKernel {

    private static final Function<boolean[], boolean[]> BOOLEAN_ARRAY_COPY = boolean[]::clone;
    private static final Function<char[], char[]> CHAR_ARRAY_COPY = char[]::clone;
    private static final Function<byte[], byte[]> BYTE_ARRAY_COPY = byte[]::clone;
    private static final Function<short[], short[]> SHORT_ARRAY_COPY = short[]::clone;
    private static final Function<int[], int[]> INT_ARRAY_COPY = int[]::clone;
    private static final Function<long[], long[]> LONG_ARRAY_COPY = long[]::clone;
    private static final Function<float[], float[]> FLOAT_ARRAY_COPY = float[]::clone;
    private static final Function<double[], double[]> DOUBLE_ARRAY_COPY = double[]::clone;
    private static final Function<Object[], Object[]> OBJECT_ARRAY_COPY = Object[]::clone;

    private static final Function<CharVector, CharVector> CHAR_VECTOR_COPY = CharVector::getDirect;
    private static final Function<ByteVector, ByteVector> BYTE_VECTOR_COPY = ByteVector::getDirect;
    private static final Function<ShortVector, ShortVector> SHORT_VECTOR_COPY = ShortVector::getDirect;
    private static final Function<IntVector, IntVector> INT_VECTOR_COPY = IntVector::getDirect;
    private static final Function<LongVector, LongVector> LONG_VECTOR_COPY = LongVector::getDirect;
    private static final Function<FloatVector, FloatVector> FLOAT_VECTOR_COPY = FloatVector::getDirect;
    private static final Function<DoubleVector, DoubleVector> DOUBLE_VECTOR_COPY = DoubleVector::getDirect;
    private static final Function<ObjectVector<?>, ObjectVector<?>> OBJECT_VECTOR_COPY = ObjectVector::getDirect;

    private static final PermuteKernel BOOLEAN_ARRAY_KERNEL = ObjectCopyPermuteKernel.of(BOOLEAN_ARRAY_COPY);
    private static final PermuteKernel CHAR_ARRAY_KERNEL = ObjectCopyPermuteKernel.of(CHAR_ARRAY_COPY);
    private static final PermuteKernel BYTE_ARRAY_KERNEL = ObjectCopyPermuteKernel.of(BYTE_ARRAY_COPY);
    private static final PermuteKernel SHORT_ARRAY_KERNEL = ObjectCopyPermuteKernel.of(SHORT_ARRAY_COPY);
    private static final PermuteKernel INT_ARRAY_KERNEL = ObjectCopyPermuteKernel.of(INT_ARRAY_COPY);
    private static final PermuteKernel LONG_ARRAY_KERNEL = ObjectCopyPermuteKernel.of(LONG_ARRAY_COPY);
    private static final PermuteKernel FLOAT_ARRAY_KERNEL = ObjectCopyPermuteKernel.of(FLOAT_ARRAY_COPY);
    private static final PermuteKernel DOUBLE_ARRAY_KERNEL = ObjectCopyPermuteKernel.of(DOUBLE_ARRAY_COPY);
    private static final PermuteKernel OBJECT_ARRAY_KERNEL = ObjectCopyPermuteKernel.of(OBJECT_ARRAY_COPY);

    private static final PermuteKernel CHAR_VECTOR_KERNEL = ObjectCopyPermuteKernel.of(CHAR_VECTOR_COPY);
    private static final PermuteKernel BYTE_VECTOR_KERNEL = ObjectCopyPermuteKernel.of(BYTE_VECTOR_COPY);
    private static final PermuteKernel SHORT_VECTOR_KERNEL = ObjectCopyPermuteKernel.of(SHORT_VECTOR_COPY);
    private static final PermuteKernel INT_VECTOR_KERNEL = ObjectCopyPermuteKernel.of(INT_VECTOR_COPY);
    private static final PermuteKernel LONG_VECTOR_KERNEL = ObjectCopyPermuteKernel.of(LONG_VECTOR_COPY);
    private static final PermuteKernel FLOAT_VECTOR_KERNEL = ObjectCopyPermuteKernel.of(FLOAT_VECTOR_COPY);
    private static final PermuteKernel DOUBLE_VECTOR_KERNEL = ObjectCopyPermuteKernel.of(DOUBLE_VECTOR_COPY);
    private static final PermuteKernel OBJECT_VECTOR_KERNEL = ObjectCopyPermuteKernel.of(OBJECT_VECTOR_COPY);

    public static PermuteKernel of(ArrayType<?, ?> arrayType) {
        return arrayType.walk(ArrayTypeVisitor.INSTANCE);
    }

    private enum ArrayTypeVisitor implements ArrayType.Visitor<PermuteKernel> {
        INSTANCE;

        @Override
        public PermuteKernel visit(NativeArrayType<?, ?> nativeArrayType) {
            return nativeArrayType.componentType().walk(NativeArrayComponentVisitor.INSTANCE);
        }

        @Override
        public PermuteKernel visit(PrimitiveVectorType<?, ?> vectorPrimitiveType) {
            return vectorPrimitiveType.componentType().walk(VectorComponentVisitor.INSTANCE);
        }

        @Override
        public PermuteKernel visit(GenericVectorType<?, ?> genericVectorType) {
            return OBJECT_VECTOR_KERNEL;
        }
    }

    private enum NativeArrayComponentVisitor implements Type.Visitor<PermuteKernel>, PrimitiveType.Visitor<PermuteKernel> {
        INSTANCE;

        @Override
        public PermuteKernel visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<PermuteKernel>) this);
        }

        @Override
        public PermuteKernel visit(GenericType<?> genericType) {
            return OBJECT_ARRAY_KERNEL;
        }

        @Override
        public PermuteKernel visit(BooleanType booleanType) {
            return BOOLEAN_ARRAY_KERNEL;
        }

        @Override
        public PermuteKernel visit(ByteType byteType) {
            return BYTE_ARRAY_KERNEL;
        }

        @Override
        public PermuteKernel visit(CharType charType) {
            return CHAR_ARRAY_KERNEL;
        }

        @Override
        public PermuteKernel visit(ShortType shortType) {
            return SHORT_ARRAY_KERNEL;
        }

        @Override
        public PermuteKernel visit(IntType intType) {
            return INT_ARRAY_KERNEL;
        }

        @Override
        public PermuteKernel visit(LongType longType) {
            return LONG_ARRAY_KERNEL;
        }

        @Override
        public PermuteKernel visit(FloatType floatType) {
            return FLOAT_ARRAY_KERNEL;
        }

        @Override
        public PermuteKernel visit(DoubleType doubleType) {
            return DOUBLE_ARRAY_KERNEL;
        }
    }

    private enum VectorComponentVisitor implements PrimitiveType.Visitor<PermuteKernel> {
        INSTANCE;

        @Override
        public PermuteKernel visit(BooleanType booleanType) {
            throw new IllegalStateException("No BooleanVector type");
        }

        @Override
        public PermuteKernel visit(ByteType byteType) {
            return BYTE_VECTOR_KERNEL;
        }

        @Override
        public PermuteKernel visit(CharType charType) {
            return CHAR_VECTOR_KERNEL;
        }

        @Override
        public PermuteKernel visit(ShortType shortType) {
            return SHORT_VECTOR_KERNEL;
        }

        @Override
        public PermuteKernel visit(IntType intType) {
            return INT_VECTOR_KERNEL;
        }

        @Override
        public PermuteKernel visit(LongType longType) {
            return LONG_VECTOR_KERNEL;
        }

        @Override
        public PermuteKernel visit(FloatType floatType) {
            return FLOAT_VECTOR_KERNEL;
        }

        @Override
        public PermuteKernel visit(DoubleType doubleType) {
            return DOUBLE_VECTOR_KERNEL;
        }
    }
}
