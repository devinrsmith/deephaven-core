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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class LinesProcessor implements Iterator<List<WritableChunk<?>>>, Closeable {

    private final JsonParser parser;
    private final ValueProcessor processor;
    private final int bufferSize;

    LinesProcessor(final JsonParser parser, final ValueProcessor processor, final int bufferSize) {
        this.parser = Objects.requireNonNull(parser);
        this.processor = Objects.requireNonNull(processor);
        this.bufferSize = bufferSize;
    }

    @Override
    public boolean hasNext() {
        return parser.hasCurrentToken();
    }

    @Override
    public List<WritableChunk<?>> next() {
        if (!parser.hasCurrentToken()) {
            throw new NoSuchElementException();
        }
        return nextUnchecked();
    }

    public List<WritableChunk<?>> nextChunks() throws IOException {
        if (!parser.hasCurrentToken()) {
            throw new NoSuchElementException();
        }
        return nextImpl();
    }

    @Override
    public void forEachRemaining(final Consumer<? super List<WritableChunk<?>>> action) {
        while (parser.hasCurrentToken()) {
            action.accept(nextUnchecked());
        }
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }

    private List<WritableChunk<?>> nextImpl() throws IOException {
        final List<WritableChunk<?>> buffer = newChunks();
        processor.setContext(buffer);
        try {
            for (int i = 0; i < bufferSize && parser.hasCurrentToken(); ++i) {
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

//    public Stream<List<WritableChunk<?>>> stream() {
//        final Spliterator<List<WritableChunk<?>>> spliterator = Spliterators.spliteratorUnknownSize(this,
//                Spliterator.NONNULL | Spliterator.ORDERED);
//        return StreamSupport.stream(spliterator, false).onClose(this::closeUnchecked);
//    }

//    private void closeUnchecked() {
//        try {
//            close();
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
//    }

    private List<WritableChunk<?>> nextUnchecked() {
        try {
            return nextImpl();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<WritableChunk<?>> newChunks() {
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
