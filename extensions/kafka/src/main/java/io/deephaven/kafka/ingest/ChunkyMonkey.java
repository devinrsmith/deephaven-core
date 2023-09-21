/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;

public interface ChunkyMonkey<T> {
    List<ChunkType> chunkTypes();

    void handle(T in, List<WritableChunk<?>> out);

    void handleChunk(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out);
}
