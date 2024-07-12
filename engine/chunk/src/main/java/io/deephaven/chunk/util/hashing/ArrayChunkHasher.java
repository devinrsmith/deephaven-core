//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import io.deephaven.math.Equals;

public final class ArrayChunkHasher {


    public static ChunkHasher of(Class<?> arrayType, Equals equals) {
        if (!arrayType.isArray()) {
            throw new IllegalArgumentException();
        }
        return new ObjectChunkFunctionHasher<>(equals.deepHashCode(arrayType));
    }
}
