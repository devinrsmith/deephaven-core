/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.util.SafeCloseable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

public final class ProcessedIterator<T> implements Iterator<List<WritableChunk<?>>>, Closeable {

    /**
     * TODO
     *
     * This is usually used with static sources, and not event streamed sources.
     *
     * @param iterator
     * @param processor
     * @param chunkSize
     * @return
     * @param <T>
     * @throws IOException
     */
    public static <T> ProcessedIterator<T> it(Iterator<T> iterator, ObjectProcessor<? super T> processor, int chunkSize) throws IOException {
        return new ProcessedIterator<>(iterator, processor, chunkSize);
    }

    private final ObjectProcessor<? super T> processor;
    private final ChunkedIterator<T> chunkedIterator;

    private ProcessedIterator(Iterator<T> in, ObjectProcessor<? super T> processor, int chunkSize) {
        this.processor = Objects.requireNonNull(processor);
        this.chunkedIterator = new ChunkedIterator<>(in, chunkSize);
    }

    @Override
    public boolean hasNext() {
        return chunkedIterator.hasNext();
    }

    /**
     * Callers are responsible for closing the returned chunks.
     *
     * @return the next chunks
     */
    @Override
    public List<WritableChunk<?>> next() {
        return processAll(chunkedIterator.next());
    }

    @Override
    public void close() {
        chunkedIterator.close();
    }

    private List<WritableChunk<?>> processAll(ObjectChunk<T, ?> chunk) {
        final List<WritableChunk<?>> out = newProcessorChunks(processor, chunk.size());
        try {
            processor.processAll(chunk, out);
        } catch (Throwable t) {
            SafeCloseable.closeAll(out.iterator());
            throw t;
        }
        chunkedIterator.processedChunk();
        return out;
    }

    private static List<WritableChunk<?>> newProcessorChunks(ObjectProcessor<?> processor, int chunkSize) {
        return processor
                .outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(chunkType -> chunkType.makeWritableChunk(chunkSize))
                .peek(wc -> wc.setSize(0))
                .collect(Collectors.toList());
    }

    // callers not responsible for closing results from next()
    // may be more generally useful later in chunking layer?
    private static final class ChunkedIterator<T> implements Iterator<ObjectChunk<T, ?>>, Closeable {
        private final Iterator<T> in;
        private final int chunkSize;
        private final WritableObjectChunk<T, ?> out;

        private ChunkedIterator(Iterator<T> in, int chunkSize) {
            this.in = Objects.requireNonNull(in);
            this.chunkSize = chunkSize;
            out = WritableObjectChunk.makeWritableChunk(chunkSize);
        }

        @Override
        public boolean hasNext() {
            return in.hasNext();
        }

        /**
         * Fills in an chunk; callers must not keep a reference to it. Callers are encouraged to call
         * {@link #processedChunk()} as soon as they are done with the returned value.
         *
         * @return the chunk
         */
        @Override
        public ObjectChunk<T, ?> next() {
            int ix;
            for (ix = 0; ix < chunkSize && in.hasNext(); ++ix) {
                out.set(ix, in.next());
            }
            if (ix == 0) {
                throw new NoSuchElementException();
            }
            out.setSize(ix);
            return out;
        }

        public void processedChunk() {
            out.fillWithNullValue(0, out.size());
            out.setSize(0);
        }

        @Override
        public void close() {
            out.close();
        }
    }
}
