//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
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

import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiPredicate;

public class BiPredicateHelper {

    private static final BiPredicate<Object, Object> IDENTITY = BiPredicateHelper::identity;
    private static final BiPredicate<Object, Object> EQUALS = Objects::equals;

    // java.util.Objects.deepEquals broken out by array type
    private static final BiPredicate<boolean[], boolean[]> BOOLEANS_EQUALS = Arrays::equals;
    private static final BiPredicate<byte[], byte[]> BYTES_EQUALS = Arrays::equals;
    private static final BiPredicate<char[], char[]> CHARS_EQUALS = Arrays::equals;
    private static final BiPredicate<short[], short[]> SHORTS_EQUALS = Arrays::equals;
    private static final BiPredicate<int[], int[]> INTS_EQUALS = Arrays::equals;
    private static final BiPredicate<long[], long[]> LONGS_EQUALS = Arrays::equals;
    private static final BiPredicate<float[], float[]> FLOATS_EQUALS = Arrays::equals;
    private static final BiPredicate<double[], double[]> DOUBLES_EQUALS = Arrays::equals;
    private static final BiPredicate<Object[], Object[]> OBJECTS_DEEP = Arrays::deepEquals;

    public static <T> BiPredicate<T, T> identity() {
        // noinspection unchecked
        return (BiPredicate<T, T>) IDENTITY;
    }

    public static <T> BiPredicate<T, T> equals(GenericType<T> type) {
        return type.walk(new ObjectEqualsVisitor<>());
    }

    public static <T> BiPredicate<T, T> deepEquals(GenericType<T> type) {
        return type.walk(new ObjectDeepEqualsVisitor<>());
    }

    private static <T> boolean identity(T a, T b) {
        return a == b;
    }

    private static <T> BiPredicate<T, T> equals() {
        // noinspection unchecked
        return (BiPredicate<T, T>) EQUALS;
    }

    private static final class ObjectEqualsVisitor<T>
            implements GenericType.Visitor<BiPredicate<T, T>>, ArrayType.Visitor<BiPredicate<T, T>> {

        @Override
        public BiPredicate<T, T> visit(BoxedType<?> boxedType) {
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(StringType stringType) {
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(InstantType instantType) {
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(CustomType<?> customType) {
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(ArrayType<?, ?> arrayType) {
            return arrayType.walk((ArrayType.Visitor<BiPredicate<T, T>>) this);
        }

        @Override
        public BiPredicate<T, T> visit(PrimitiveVectorType<?, ?> vectorPrimitiveType) {
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(GenericVectorType<?, ?> genericVectorType) {
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(NativeArrayType<?, ?> nativeArrayType) {
            // java.util.Objects.equals for arrays is equivalent to identity comparison
            return BiPredicateHelper.identity();
        }
    }

    private static final class ObjectDeepEqualsVisitor<T>
            implements GenericType.Visitor<BiPredicate<T, T>>, ArrayType.Visitor<BiPredicate<T, T>> {

        @Override
        public BiPredicate<T, T> visit(BoxedType<?> boxedType) {
            // java.util.Objects.deepEquals for non array types is equivalent to java.util.Objects.equals comparison
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(StringType stringType) {
            // java.util.Objects.deepEquals for non array types is equivalent to java.util.Objects.equals comparison
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(InstantType instantType) {
            // java.util.Objects.deepEquals for non array types is equivalent to java.util.Objects.equals comparison
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(CustomType<?> customType) {
            // java.util.Objects.deepEquals for non array types is equivalent to java.util.Objects.equals comparison
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(ArrayType<?, ?> arrayType) {
            return arrayType.walk((ArrayType.Visitor<BiPredicate<T, T>>) this);
        }

        @Override
        public BiPredicate<T, T> visit(PrimitiveVectorType<?, ?> vectorPrimitiveType) {
            // java.util.Objects.deepEquals for non array types is equivalent to java.util.Objects.equals comparison
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(GenericVectorType<?, ?> genericVectorType) {
            // java.util.Objects.deepEquals for non array types is equivalent to java.util.Objects.equals comparison
            return BiPredicateHelper.equals();
        }

        @Override
        public BiPredicate<T, T> visit(NativeArrayType<?, ?> nativeArrayType) {
            return nativeArrayType.componentType().walk(new ObjectDeepEqualsNativeArrayComponentVisitor<>());
        }
    }

    private static final class ObjectDeepEqualsNativeArrayComponentVisitor<T>
            implements Type.Visitor<BiPredicate<T, T>>, PrimitiveType.Visitor<BiPredicate<T, T>> {

        @Override
        public BiPredicate<T, T> visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<BiPredicate<T, T>>) this);
        }

        @Override
        public BiPredicate<T, T> visit(GenericType<?> genericType) {
            // noinspection unchecked
            return (BiPredicate<T, T>) OBJECTS_DEEP;
        }

        @Override
        public BiPredicate<T, T> visit(BooleanType booleanType) {
            // noinspection unchecked
            return (BiPredicate<T, T>) BOOLEANS_EQUALS;
        }

        @Override
        public BiPredicate<T, T> visit(ByteType byteType) {
            // noinspection unchecked
            return (BiPredicate<T, T>) BYTES_EQUALS;
        }

        @Override
        public BiPredicate<T, T> visit(CharType charType) {
            // noinspection unchecked
            return (BiPredicate<T, T>) CHARS_EQUALS;
        }

        @Override
        public BiPredicate<T, T> visit(ShortType shortType) {
            // noinspection unchecked
            return (BiPredicate<T, T>) SHORTS_EQUALS;
        }

        @Override
        public BiPredicate<T, T> visit(IntType intType) {
            // noinspection unchecked
            return (BiPredicate<T, T>) INTS_EQUALS;
        }

        @Override
        public BiPredicate<T, T> visit(LongType longType) {
            // noinspection unchecked
            return (BiPredicate<T, T>) LONGS_EQUALS;
        }

        @Override
        public BiPredicate<T, T> visit(FloatType floatType) {
            // noinspection unchecked
            return (BiPredicate<T, T>) FLOATS_EQUALS;
        }

        @Override
        public BiPredicate<T, T> visit(DoubleType doubleType) {
            // noinspection unchecked
            return (BiPredicate<T, T>) DOUBLES_EQUALS;
        }
    }
}
