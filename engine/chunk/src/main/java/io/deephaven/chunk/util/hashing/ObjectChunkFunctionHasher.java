//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.HashCodes;
import io.deephaven.chunk.attributes.Values;

import java.util.Objects;
import java.util.function.ToIntFunction;

import static io.deephaven.chunk.util.hashing.ChunkHasher.scrambleHash;

public final class ObjectChunkFunctionHasher<T> implements ChunkHasher {
    private final ToIntFunction<T> hashFunction;

    public ObjectChunkFunctionHasher(ToIntFunction<T> hashFunction) {
        this.hashFunction = Objects.requireNonNull(hashFunction);
    }

    @Override
    public int hashInitial(Object value) {
        // noinspection unchecked
        return hashInitialSingle(hashFunction, (T) value);
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        // noinspection unchecked
        return hashUpdateSingle(hashFunction, existing, (T) value);
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashInitial(hashFunction, values.asObjectChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashSecondary(hashFunction, values.asObjectChunk(), destination);
    }

    private static <T> void hashInitial(ToIntFunction<T> hashFunction, ObjectChunk<T, Values> values,
            WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashInitialSingle(hashFunction, values.get(ii)));
        }
        destination.setSize(values.size());
    }

    private static <T> void hashSecondary(ToIntFunction<T> hashFunction, ObjectChunk<T, Values> values,
            WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateSingle(hashFunction, destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    private static <T> int hashInitialSingle(ToIntFunction<T> hashFunction, T value) {
        return scrambleHash(hashFunction.applyAsInt(value));
    }

    private static <T> int hashUpdateSingle(ToIntFunction<T> hashFunction, int existing, T newValue) {
        return existing * 31 + hashInitialSingle(hashFunction, newValue);
    }
}
