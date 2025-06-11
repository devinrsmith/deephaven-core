//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.util.SafeCloseable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class JacksonIterator implements Iterator<List<WritableChunk<?>>> {

    protected final JsonParser parser;
    private final ValueProcessor processor;
    private final int bufferSize;

    JacksonIterator(final ValueProcessor processor, final JsonParser parser, final int bufferSize) {
        this.parser = Objects.requireNonNull(parser);
        this.processor = Objects.requireNonNull(processor);
        this.bufferSize = bufferSize;
    }

    @Override
    public final List<WritableChunk<?>> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextUnchecked();
    }

    public final List<WritableChunk<?>> nextChunks() throws IOException {
        return nextChunks(bufferSize);
    }

    public final List<WritableChunk<?>> nextChunks(final int maxItems) throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextImpl(maxItems);
    }

    @Override
    public final void forEachRemaining(final Consumer<? super List<WritableChunk<?>>> action) {
        while (hasNext()) {
            action.accept(nextUnchecked());
        }
    }

    private List<WritableChunk<?>> nextImpl(final int maxItems) throws IOException {
        final List<WritableChunk<?>> buffer = newChunks(maxItems);
        processor.setContext(buffer);
        try {
            for (int i = 0; i < maxItems && hasNext(); ++i) {
                processCurrent(buffer);
            }
        } finally {
            processor.clearContext();
        }
        return buffer;
    }

    private void processCurrent(final List<WritableChunk<?>> buffer) throws IOException {
        try {
            processor.processCurrentValue(parser);
            parser.nextToken();
        } catch (final IOException | RuntimeException e) {
            try {
                SafeCloseable.closeAll(buffer.iterator());
            } catch (final RuntimeException e2) {
                e.addSuppressed(e2);
            }
            throw e;
        }
    }

    private List<WritableChunk<?>> nextUnchecked() {
        try {
            return nextImpl(bufferSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<WritableChunk<?>> newChunks(final int maxItems) {
        return processor.columnTypes()
                .map(ObjectProcessor::chunkType)
                .map(x -> chunk(x, maxItems))
                .collect(Collectors.toList());
    }

    private static WritableChunk<Any> chunk(final ChunkType chunkType, final int maxItems) {
        final WritableChunk<Any> chunk = chunkType.makeWritableChunk(maxItems);
        chunk.setSize(0);
        return chunk;
    }
}
