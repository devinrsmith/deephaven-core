/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.ToShortFunction;
import io.deephaven.kafka.ingest.ChunkUtils;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;

import java.util.List;
import java.util.Objects;

final class KeyOrValueProcessorImpl2<T> implements KeyOrValueProcessor {

    private final int[] chunkOffsets;
    private final List<CopyFunction<T>> functions;
    private final int maxChunkSize;


    interface CopyFunction<T> {
        void applyInto(ObjectChunk<T, Values> src, int srcOffset, WritableChunk<Values> dest, int destOffset, int length);
    }

    @Override
    public void handleChunk(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values>[] publisherChunks) {
        //noinspection unchecked
        final ObjectChunk<T, Values> src = (ObjectChunk<T, Values>) inputChunk;
        final int totalInputRows = inputChunk.size();
        for (int rowIx = 0; rowIx < totalInputRows; rowIx += maxChunkSize) {
            final int chunkSize = Math.min(totalInputRows - rowIx, maxChunkSize);
            for (int columnIx = 0; columnIx < chunkOffsets.length; ++columnIx) {
                final WritableChunk<Values> dest = publisherChunks[chunkOffsets[columnIx]];
                final int existingSize = dest.size();
                dest.setSize(existingSize + chunkSize);
                functions.get(columnIx).applyInto(src, rowIx, dest, existingSize, chunkSize);
            }
        }
    }

    private static class ShortDolt<T> implements CopyFunction<T> {
        private final ToShortFunction<T> toShortFunction;

        public ShortDolt(ToShortFunction<T> toShortFunction) {
            this.toShortFunction = Objects.requireNonNull(toShortFunction);
        }

        @Override
        public void applyInto(ObjectChunk<T, Values> src, int srcOffset, WritableChunk<Values> dest, int destOffset, int length) {
            ChunkUtils.applyInto(toShortFunction, src, srcOffset, dest.asWritableShortChunk(), destOffset, length);
        }
    }

}
