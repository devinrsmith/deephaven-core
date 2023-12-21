/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

class Impl1<T> implements Accum<T> {

    private final ObjectProcessor<T> processor;
    private final WritableObjectChunk<T, ?> in;
    private final List<WritableChunk<Values>[]> out;
    private final int chunkSize;
    private final StreamConsumer consumer = null;


    public Impl1(ObjectProcessor<T> processor, int chunkSize) {
        this.processor = Objects.requireNonNull(processor);
        this.in = WritableObjectChunk.writableChunkWrap(WritableObjectChunk.makeArray(chunkSize));
        this.in.setSize(0);
        this.out = new ArrayList<>();
        this.chunkSize = chunkSize;
    }

    public synchronized void accumulate(Iterable<T> src) {
        for (T record : src) {
            in.add(record);
            if (in.size() == chunkSize) {
                process();
            }
        }
    }

    public synchronized void accumulate(T[] src, int offset, int len) {
        int outstandingRemaining = chunkSize - in.size();
        while (len >= outstandingRemaining) {
            in.copyFromTypedArray(src, offset, in.size(), outstandingRemaining);
            in.setSize(chunkSize);
            process();
            offset += outstandingRemaining;
            len -= outstandingRemaining;
            outstandingRemaining = chunkSize;
        }
        in.copyFromTypedArray(src, offset, in.size(), len);
        in.setSize(in.size() + len);
    }

    private void process() {
        final WritableChunk<Values>[] outChunks = makeWritableChunks(in.size());
        processor.processAll(in, Arrays.asList(outChunks));
        in.fillWithNullValue(0, in.size());
        in.setSize(0);
        out.add(outChunks);
    }

    public synchronized void flush() {
        if (in.size() > 0) {
            process();
        }
        consumer.accept(out);
        out.clear();
    }

    private WritableChunk<Values>[] makeWritableChunks(int size) {
        // noinspection unchecked
        final WritableChunk<Values>[] chunks = processor
                .outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(c -> c.<Values>makeWritableChunk(size))
                .toArray(WritableChunk[]::new);
        for (WritableChunk<?> chunk : chunks) {
            if (chunk.size() != size) {
                throw new IllegalStateException("Expected making chunks to return the exact same size...");
            }
            chunk.setSize(0);
        }
        return chunks;
    }
}
