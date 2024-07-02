package io.deephaven.chunk.util.hashing;

import java.util.Arrays;
import java.util.function.BiPredicate;

public final class ArrayChunkEquals {

    private static final BiPredicate<boolean[], boolean[]> BOOLEAN_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<char[], char[]> CHAR_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<byte[], byte[]> BYTE_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<short[], short[]> SHORT_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<int[], int[]> INT_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<long[], long[]> LONG_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<float[], float[]> FLOAT_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<double[], double[]> DOUBLE_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<Object[], Object[]> OBJECT_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<Object[], Object[]> OBJECT_ARRAY_DEEP_EQUALS = Arrays::deepEquals;

    private static final ChunkEquals BOOLEAN_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(BOOLEAN_ARRAY_EQUALS);
    private static final ChunkEquals CHAR_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(CHAR_ARRAY_EQUALS);
    private static final ChunkEquals BYTE_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(BYTE_ARRAY_EQUALS);
    private static final ChunkEquals SHORT_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(SHORT_ARRAY_EQUALS);
    private static final ChunkEquals INT_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(INT_ARRAY_EQUALS);
    private static final ChunkEquals LONG_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(LONG_ARRAY_EQUALS);
    private static final ChunkEquals FLOAT_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(FLOAT_ARRAY_EQUALS);
    private static final ChunkEquals DOUBLE_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(DOUBLE_ARRAY_EQUALS);
    private static final ChunkEquals OBJECT_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(OBJECT_ARRAY_EQUALS);
    private static final ChunkEquals OBJECT_ARRAY_CHUNK_DEEP_EQUALS = new ObjectChunkPredicateEquals<>(OBJECT_ARRAY_DEEP_EQUALS);

    public static ChunkEquals of() {
        return null;
    }
}
