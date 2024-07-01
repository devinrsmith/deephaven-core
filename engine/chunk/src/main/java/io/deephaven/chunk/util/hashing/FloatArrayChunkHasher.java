//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharArrayChunkHasher and run "./gradlew replicateHashing" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.hashing;


import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.HashCodes;
import io.deephaven.chunk.attributes.Values;

import java.util.Arrays;

import static io.deephaven.chunk.util.hashing.ChunkHasher.scrambleHash;


public class FloatArrayChunkHasher implements ChunkHasher {
    public static final FloatArrayChunkHasher INSTANCE = new FloatArrayChunkHasher();

    private static void hashInitial(ObjectChunk<float[], Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final float[] value = values.get(ii);
            destination.set(ii, hashInitialSingle(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(ObjectChunk<float[], Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateSingle(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    public static int hashInitialSingle(float[] value) {
        return scrambleHash(Arrays.hashCode(value));
    }

    public static int hashUpdateSingle(int existing, float[] newValue) {
        return existing * 31 + hashInitialSingle(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialSingle((float[]) value);
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateSingle(existing, (float[]) value);
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashInitial(values.asObjectChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashSecondary(values.asObjectChunk(), destination);
    }
}
