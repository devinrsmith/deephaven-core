/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

class ObjectProcessorStrict<T> implements ObjectProcessor<T> {

    static <T> ObjectProcessor<T> create(ObjectProcessor<T> delegate) {
        if (delegate instanceof ObjectProcessorStrict) {
            return delegate;
        }
        return new ObjectProcessorStrict<>(delegate);
    }

    private final ObjectProcessor<T> delegate;

    ObjectProcessorStrict(ObjectProcessor<T> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return delegate.outputTypes();
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        final int numColumns = delegate.outputTypes().size();
        if (numColumns != out.size()) {
            throw new IllegalArgumentException(String.format(
                    "Expected delegate.outputTypes().size() == out.size(). delegate.outputTypes().size()=%d, out.size()=%d",
                    numColumns, out.size()));
        }
        final int[] originalSizes = new int[numColumns];
        for (int i = 0; i < numColumns; ++i) {
            final WritableChunk<?> chunk = out.get(i);
            if (chunk.capacity() - chunk.size() < in.size()) {
                throw new IllegalArgumentException(String.format(
                        "out chunk does not have enough remaining capacity. i=%d, in.size()=%d, chunk.size()=%d, chunk.capacity()=%d",
                        i, in.size(), chunk.size(), chunk.capacity()));
            }
            final Type<?> type = delegate.outputTypes().get(i);
            final ChunkType expectedChunkType = ObjectProcessor.chunkType(type);
            final ChunkType actualChunkType = chunk.getChunkType();
            if (!expectedChunkType.equals(actualChunkType)) {
                throw new IllegalArgumentException(String.format(
                        "Improper ChunkType. i=%d, outputType=%s, expectedChunkType=%s, actualChunkType=%s", i, type,
                        expectedChunkType, actualChunkType));
            }
            originalSizes[i] = chunk.size();
        }
        delegate.processAll(in, out);
        for (int i = 0; i < numColumns; ++i) {
            final WritableChunk<?> chunk = out.get(i);
            final int expectedSize = originalSizes[i] + in.size();
            if (chunk.size() != expectedSize) {
                throw new UncheckedDeephavenException(String.format(
                        "Implementation did not increment chunk size correctly. i=%d, (before) chunk.size()=%d, (after) chunk.size()=%d, in.size()=%d",
                        i, originalSizes[i], chunk.size(), in.size()));
            }
        }
    }
}
