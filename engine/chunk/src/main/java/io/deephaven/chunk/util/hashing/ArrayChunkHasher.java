//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import java.util.Arrays;
import java.util.function.ToIntFunction;

public final class ArrayChunkHasher {

    // todo: need to consider float / double and DH treatment of them

    private static final ToIntFunction<boolean[]> BOOLEAN_ARRAY_HASH = Arrays::hashCode;
    private static final ToIntFunction<char[]> CHAR_ARRAY_HASH = Arrays::hashCode;
    private static final ToIntFunction<byte[]> BYTE_ARRAY_HASH = Arrays::hashCode;
    private static final ToIntFunction<short[]> SHORT_ARRAY_HASH = Arrays::hashCode;
    private static final ToIntFunction<int[]> INT_ARRAY_HASH = Arrays::hashCode;
    private static final ToIntFunction<long[]> LONG_ARRAY_HASH = Arrays::hashCode;
    private static final ToIntFunction<float[]> FLOAT_ARRAY_HASH = Arrays::hashCode;
    private static final ToIntFunction<double[]> DOUBLE_ARRAY_HASH = Arrays::hashCode;
    private static final ToIntFunction<Object[]> OBJECT_ARRAY_HASH = Arrays::hashCode;
    private static final ToIntFunction<Object[]> OBJECT_ARRAY_DEEP_HASH = Arrays::deepHashCode;

    private static final ChunkHasher BOOLEAN_ARRAY_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);
    private static final ChunkHasher CHAR_ARRAY_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);
    private static final ChunkHasher BYTE_ARRAY_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);
    private static final ChunkHasher SHORT_ARRAY_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);
    private static final ChunkHasher INT_ARRAY_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);
    private static final ChunkHasher LONG_ARRAY_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);
    private static final ChunkHasher FLOAT_ARRAY_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);
    private static final ChunkHasher DOUBLE_ARRAY_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);
    private static final ChunkHasher OBJECT_ARRAY_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);
    private static final ChunkHasher OBJECT_ARRAY_DEEP_HASHER = new ObjectChunkFunctionHasher<>(BOOLEAN_ARRAY_HASH);


}
