//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSink.FillFromContext;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.util.SafeCloseable;

import java.util.ArrayList;
import java.util.Arrays;
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
            List<WritableColumnSource<?>> dstColumnSources,
            RowSet dstRowSet,
            int chunkSize) {
        final int numColumns = processor.size();
        if (dstColumnSources.size() != numColumns) {
            throw new IllegalArgumentException(
                    String.format("Unexpected sizes, processor.size() != dstColumnSources.size(): %d != %d", numColumns,
                            dstColumnSources.size()));
        }
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
        final FillFromContext[] fillFromContexts = dstColumnSources.stream()
                .map(cs -> cs.makeFillFromContext(chunkSize))
                .toArray(FillFromContext[]::new);
        final List<WritableChunk<Values>> intermediateChunks = makeChunks(processor, chunkSize).collect(Collectors.toList());
        try (
                final WritableObjectChunk<? extends T, Values> src = WritableObjectChunk.makeWritableChunk(chunkSize);
                final FillContext srcFillContext = srcColumnSource.makeFillContext(chunkSize);
                final Iterator srcIt = srcRowSet.getRowSequenceIterator();
                final Iterator dstIt = dstRowSet.getRowSequenceIterator()) {
            while (srcIt.hasMore() || dstIt.hasMore()) {
                final RowSequence srcSeq = srcIt.getNextRowSequenceWithLength(chunkSize);
                final RowSequence dstSeq = dstIt.getNextRowSequenceWithLength(chunkSize);
                final int rowSeqSize = srcSeq.intSize();
                if (dstSeq.intSize() != rowSeqSize) {
                    throw new IllegalArgumentException(
                            "srcRowSet (iterator) is not the same size as dstRowSet (iterator)");
                }
                if (srcUsePrev) {
                    srcColumnSource.fillPrevChunk(srcFillContext, src, srcSeq);
                } else {
                    srcColumnSource.fillChunk(srcFillContext, src, srcSeq);
                }
                for (WritableChunk<?> chunk : intermediateChunks) {
                    chunk.setSize(0);
                }
                // noinspection rawtypes,unchecked
                processor.processAll(src, (List) intermediateChunks);
                for (int i = 0; i < numColumns; ++i) {
                    final WritableChunk<Values> chunk = intermediateChunks.get(i);
                    if (chunk.size() != rowSeqSize) {
                        throw new IllegalStateException();
                    }
                    dstColumnSources.get(i).fillFromChunk(fillFromContexts[i], chunk, dstSeq);
                }
            }
        } finally {
            SafeCloseable.closeAll(Stream.concat(intermediateChunks.stream(), Stream.of(fillFromContexts)));
        }
    }

    public static <T> void processAll2(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            List<WritableColumnSource<?>> dstColumnSources,
            RowSet dstRowSet,
            int chunkSize) {
        try (final WhatDstColumnSource what = new WhatDstColumnSource()) {
            processAllImpl2(srcColumnSource, srcRowSet, srcUsePrev, processor, chunkSize, what);
        }
    }

    public static <T> void processAll(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            int chunkSize,
            StreamConsumer dst,
            boolean oneShot) {
        if (oneShot) {
            processAllOneShot(srcColumnSource, srcRowSet, srcUsePrev, processor, chunkSize, dst);
        } else {
            processAll(srcColumnSource, srcRowSet, srcUsePrev, processor, chunkSize, dst);
        }
    }

    public static <T> void processAll(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            int chunkSize,
            StreamConsumer dst) {
        //noinspection unchecked
        processAllImpl(srcColumnSource, srcRowSet, srcUsePrev, processor, chunkSize, dst::accept);
    }

    public static <T> void processAllOneShot(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            int chunkSize,
            StreamConsumer dst) {
        final List<WritableChunk<Values>[]> chunks = new ArrayList<>();
        try {
            processAllImpl(srcColumnSource, srcRowSet, srcUsePrev, processor, chunkSize, chunks::add);
        } catch (Throwable t) {
            SafeCloseable.closeAll(chunks.stream().flatMap(Arrays::stream));
            throw t;
        }
        dst.accept(chunks);
    }

    private static <T> void processAllImpl(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            int chunkSize,
            Consumer<WritableChunk<Values>[]> dst) {
        try (
                final WritableObjectChunk<T, Values> src = WritableObjectChunk.makeWritableChunk(chunkSize);
                final FillContext context = srcColumnSource.makeFillContext(chunkSize);
                final Iterator srcIt = srcRowSet.getRowSequenceIterator()) {
            while (srcIt.hasMore()) {
                final RowSequence rowSeq = srcIt.getNextRowSequenceWithLength(chunkSize);
                if (srcUsePrev) {
                    srcColumnSource.fillPrevChunk(context, src, rowSeq);
                } else {
                    srcColumnSource.fillChunk(context, src, rowSeq);
                }
                //noinspection unchecked
                final WritableChunk<Values>[] dstChunks = makeChunks(processor, chunkSize)
                        .peek(o -> o.setSize(0))
                        .toArray(WritableChunk[]::new);
                try {
                    processor.processAll(src, Arrays.asList(dstChunks));
                } catch (Throwable t) {
                    SafeCloseable.closeAll(dstChunks);
                    throw t;
                }
                dst.accept(dstChunks);
            }
        }
    }

    private static <T> void processAllImpl2(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            boolean srcUsePrev,
            ObjectProcessor<? super T> processor,
            int chunkSize,
            What dst) {
        try (
                final WritableObjectChunk<T, Values> src = WritableObjectChunk.makeWritableChunk(chunkSize);
                final FillContext context = srcColumnSource.makeFillContext(chunkSize);
                final Iterator srcIt = srcRowSet.getRowSequenceIterator()) {
            while (srcIt.hasMore()) {
                final RowSequence rowSeq = srcIt.getNextRowSequenceWithLength(chunkSize);
                if (srcUsePrev) {
                    srcColumnSource.fillPrevChunk(context, src, rowSeq);
                } else {
                    srcColumnSource.fillChunk(context, src, rowSeq);
                }
                final int srcSize = src.size();
                final List<WritableChunk<?>> chunks = dst.chunks(srcSize);
                try {
                    processor.processAll(src, chunks);
                } catch (Throwable t) {
                    // todo: should this be ours or What responsibility?
                    SafeCloseable.closeAll(chunks.stream());
                    throw t;
                }
                dst.accept(srcSize, chunks);
            }
        }
    }

    interface What {

        List<WritableChunk<?>> chunks(int chunkSize);

        void accept(int size, List<WritableChunk<?>> out);
    }

    private static class WhatDstColumnSource implements What, AutoCloseable {
        private final List<WritableColumnSource<?>> dstColumnSources;
        private final RowSet dstRowSet;

        private final Iterator dstIt;
        private final int numColumns;
        private final FillFromContext[] fillFromContexts;

        @Override
        public List<WritableChunk<?>> chunks(int chunkSize) {
            return null;
        }

        @Override
        public void accept(int size, List<WritableChunk<?>> out) {
            final RowSequence dstSeq = dstIt.getNextRowSequenceWithLength(size);
            if (dstSeq.intSize() != size) {
                throw new IllegalArgumentException("");
            }
            for (int i = 0; i < numColumns; ++i) {
                //noinspection unchecked
                dstColumnSources.get(i).fillFromChunk(fillFromContexts[i], (Chunk<? extends Values>)out.get(i), dstSeq);
            }
        }

        @Override
        public void close() {
            dstIt.close();
        }
    }

    private static <T> Stream<WritableChunk<Values>> makeChunks(ObjectProcessor<? super T> processor, int chunkSize) {
        return processor.outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(o -> o.makeWritableChunk(chunkSize));
    }
}
