//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSink.FillFromContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.util.SafeCloseable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class TableProcessorImpl {

    public static <T> void processAll(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            Collection<WritableColumnSource<?>> dstColumnSources,
            RowSet dstRowSet,
            int chunkSize) {
        if (srcRowSet.size() != dstRowSet.size()) {
            throw new IllegalArgumentException("");
        }
        final int numColumns = processor.size();
        if (dstColumnSources.size() != numColumns) {
            throw new IllegalArgumentException("");
        }
        final List<WritableChunk<?>> intermediates = processor.outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(o -> o.makeWritableChunk(chunkSize))
                .collect(Collectors.toList());
        final FillFromContext[] fillContexts = dstColumnSources.stream()
                .map(cs -> cs.makeFillFromContext(chunkSize))
                .toArray(FillFromContext[]::new);
        try (
                final CloseableIterator<ObjectChunk<T, Values>> srcChunkIt =
                        ColumnSourceHelper.readFilledChunks(srcColumnSource, srcRowSet, srcUsePrev, chunkSize);
                final CloseableIterator<RowSequence> dstRowSeqIt =
                        ColumnSourceHelper.rowSequenceIterator(dstRowSet, chunkSize)) {
            while (srcChunkIt.hasNext() && dstRowSeqIt.hasNext()) {
                final ObjectChunk<? extends T, Values> srcChunk = srcChunkIt.next();
                final RowSequence dstSeq = dstRowSeqIt.next();
                if (srcChunk.size() != dstSeq.size()) {
                    throw new IllegalStateException();
                }
                for (final WritableChunk<?> intermediate : intermediates) {
                    intermediate.setSize(0);
                }
                processor.processAll(srcChunk, intermediates);
                int i = 0;
                for (final WritableColumnSource<?> dstColumnSource : dstColumnSources) {
                    // noinspection unchecked
                    dstColumnSource.fillFromChunk(fillContexts[i], (Chunk<? extends Values>) intermediates.get(i),
                            dstSeq);
                    ++i;
                }
            }
            if (srcChunkIt.hasNext() || dstRowSeqIt.hasNext()) {
                throw new IllegalStateException();
            }
        } finally {
            SafeCloseable.closeAll(Stream.concat(
                    intermediates.stream(),
                    Arrays.stream(fillContexts)));
        }
    }

    public static <T> void processAll(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            int chunkSize,
            StreamConsumer dst) {
        // noinspection unchecked
        processAll(srcColumnSource, srcRowSet, srcUsePrev, processor, chunkSize, dst::accept);
    }

    public static <T> void processAllOneShot(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            int chunkSize,
            StreamConsumer dst) {
        final List<WritableChunk<Values>[]> all = new ArrayList<>();
        try {
            processAll(srcColumnSource, srcRowSet, srcUsePrev, processor, chunkSize, all::add);
        } catch (Throwable t) {
            SafeCloseable.closeAll(all.stream().flatMap(Arrays::stream));
            throw t;
        }
        dst.accept(all);
    }

    public static <T> void processAll(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            int chunkSize,
            Consumer<WritableChunk<Values>[]> consumer) {
        try (final CloseableIterator<ObjectChunk<T, Values>> srcChunkIt =
                ColumnSourceHelper.readFilledChunks(srcColumnSource, srcRowSet, srcUsePrev, chunkSize)) {
            while (srcChunkIt.hasNext()) {
                final ObjectChunk<T, Values> srcChunk = srcChunkIt.next();
                // noinspection unchecked
                final WritableChunk<Values>[] newChunks = processor.outputTypes()
                        .stream()
                        .map(ObjectProcessor::chunkType)
                        .map(o -> o.makeWritableChunk(chunkSize))
                        .peek(o -> o.setSize(0))
                        .toArray(WritableChunk[]::new);
                try {
                    processor.processAll(srcChunk, Arrays.asList(newChunks));
                } catch (Throwable t) {
                    SafeCloseable.closeAll(Arrays.stream(newChunks));
                    throw t;
                }
                consumer.accept(newChunks);
            }
        }
    }
}
