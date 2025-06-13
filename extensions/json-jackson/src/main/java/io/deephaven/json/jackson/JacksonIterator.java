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
import java.util.stream.Collectors;

/**
 * An {@link Iterator} capable of chunk iteration.
 */
public abstract class JacksonIterator implements Iterator<List<WritableChunk<?>>> {

    protected final JsonParser parser;
    private final ValueProcessor processor;
    private final int chunkCapacity;

    JacksonIterator(final ValueProcessor processor, final JsonParser parser, final int chunkCapacity) {
        if (chunkCapacity <= 0) {
            throw new IllegalArgumentException("chunkCapacity must be positive");
        }
        this.parser = Objects.requireNonNull(parser);
        this.processor = Objects.requireNonNull(processor);
        this.chunkCapacity = chunkCapacity;
    }

    /**
     * The capacity of the chunks that will be returned as part of a {@link #nextChunks()} or {@link #next()} call.
     *
     * @return the chunk capacity
     */
    public final int chunkCapacity() {
        return chunkCapacity;
    }

    /**
     * The next chunks. Ownership of the chunks passes to the caller.
     *
     * @return the next chunks
     * @throws IOException if an IO exception occurs
     * @throws NoSuchElementException if the iteration has no more elements
     */
    public final List<WritableChunk<?>> nextChunks() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextImpl();
    }

    /**
     * The next chunks. Ownership of the chunks passes to the caller. When using this in an IO-aware context, prefer
     * {@link #nextChunks()}.
     *
     * @return the next chunks
     * @throws UncheckedIOException if an IO exception occurs
     */
    @Override
    public final List<WritableChunk<?>> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            return nextImpl();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<WritableChunk<?>> nextImpl() throws IOException {
        final List<WritableChunk<?>> buffer = newChunks();
        processor.setContext(buffer);
        try {
            int i = 0;
            do {
                processor.processCurrentValue(parser);
                parser.nextToken();
                ++i;
            } while (i < chunkCapacity && hasNext());
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
        final WritableChunk<Any> chunk = chunkType.makeWritableChunk(chunkCapacity);
        chunk.setSize(0);
        return chunk;
    }
}
