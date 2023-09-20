/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.kafka.ingest.FieldCopier;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;

import java.util.List;
import java.util.Objects;

final class KeyOrValueProcessorImpl2 implements KeyOrValueProcessor {

    private final int[] chunkOffsets;
    private final List<FieldCopier> copiers;

    private final int maxChunkSize;

    public KeyOrValueProcessorImpl2(final int[] chunkOffsets, List<FieldCopier> copiers) {
        this.chunkOffsets = Objects.requireNonNull(chunkOffsets);
        this.copiers = Objects.requireNonNull(copiers);
    }

    @Override
    public void handleChunk(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values>[] publisherChunks) {
        final int totalInputRows = inputChunk.size();
        for (int chunkIx = 0; chunkIx < chunkOffsets.length; ++chunkIx) {

            final int chunkSize = Math.min()


            for (int rowIx = 0; rowIx < totalInputRows; rowIx += maxChunkSize) {
                final WritableChunk<Values> publisherChunk = publisherChunks[chunkOffsets[chunkIx]];
                final int existingSize = publisherChunk.size();


                publisherChunk.setSize(existingSize + totalInputRows);
                copiers.get(chunkIx).copyField(inputChunk, publisherChunk, 0, existingSize, totalInputRows);
            }



        }
    }

    private static class What {

    }
}
