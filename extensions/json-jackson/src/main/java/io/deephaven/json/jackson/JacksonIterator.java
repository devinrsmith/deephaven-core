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
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class JacksonIterator implements Iterator<List<WritableChunk<?>>> {

    protected final JsonParser parser;
    private final ValueProcessor processor;
    private final int bufferSize;

    JacksonIterator(final ValueProcessor processor, final JsonParser parser, final int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be positive");
        }
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
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextImpl();
    }

    @Override
    public final void forEachRemaining(final Consumer<? super List<WritableChunk<?>>> action) {
        while (hasNext()) {
            action.accept(nextUnchecked());
        }
    }

    private List<WritableChunk<?>> nextUnchecked() {
        try {
            return nextImpl();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<WritableChunk<?>> nextImpl() throws IOException {
        final List<WritableChunk<?>> buffer = newChunks();
        processor.setContext(buffer);
        try {
            for (int i = 0; i < bufferSize && hasNext(); ++i) {
                processor.processCurrentValue(parser);
                parser.nextToken();
            }
        } catch (final IOException | RuntimeException e) {
            try {
                SafeCloseable.closeAll(buffer.iterator());
            } catch (final RuntimeException e2) {
                e.addSuppressed(e2);
            }
            throw e;
        } finally {
            processor.clearContext();
        }
        return buffer;
    }

    private List<WritableChunk<?>> newChunks() {
        // StreamChunkUtils.makeChunksForDefinition(tableDefinition, CHUNK_SIZE);
        return processor.columnTypes()
                .map(ObjectProcessor::chunkType)
                .map(this::chunk)
                .collect(Collectors.toList());
    }

    private WritableChunk<Any> chunk(final ChunkType chunkType) {
        final WritableChunk<Any> chunk = chunkType.makeWritableChunk(bufferSize);
        chunk.setSize(0);
        return chunk;
    }
}
