//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink.FillFromContext;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.util.SafeCloseable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.engine.table.impl.processor.ColumnSourceHelper.rowSequenceIterator;

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
        final int numColumns = processor.outputSize();
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
                // todo: why am I using fillChunk instead of getChunk?
                final CloseableIterator<ObjectChunk<T, Values>> srcChunkIt =
                        ColumnSourceHelper.readFilledChunks(srcColumnSource, srcRowSet, srcUsePrev, chunkSize);
                final CloseableIterator<RowSequence> dstRowSeqIt =
                        rowSequenceIterator(dstRowSet, chunkSize)) {
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

    public static <T> boolean processSubset(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
            ObjectProcessor<? super T> processor,
            Collection<WritableColumnSource<?>> dstColumnSources,
            RowSet dstRowSet,
            int chunkSize,
            BiPredicate<T, T> includePredicate) {
        if (srcRowSet.size() != dstRowSet.size()) {
            throw new IllegalArgumentException("");
        }
        if (dstColumnSources.size() != processor.outputSize()) {
            throw new IllegalArgumentException("");
        }
        if (srcRowSet.isEmpty()) {
            return false;
        }
        final List<WritableChunk<?>> intermediates = processor.outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(o -> o.makeWritableChunk(chunkSize))
                .collect(Collectors.toList());
        final FillFromContext[] fillContexts = dstColumnSources.stream()
                .map(cs -> cs.makeFillFromContext(chunkSize))
                .toArray(FillFromContext[]::new);
        boolean processed = false;
        try (
                final FillContext context = srcColumnSource.makeFillContext(chunkSize);
                final WritableObjectChunk<T, Values> prev = WritableObjectChunk.makeWritableChunk(chunkSize);
                final WritableObjectChunk<T, Values> curr = WritableObjectChunk.makeWritableChunk(chunkSize);
                final WritableObjectChunk<T, Values> buffer1 = WritableObjectChunk.makeWritableChunk(chunkSize);
                final WritableObjectChunk<T, Values> buffer2 = WritableObjectChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<RowKeys> dstKeys = WritableLongChunk.makeWritableChunk(chunkSize);
                final CloseableIterator<RowSequence> srcIt = rowSequenceIterator(srcRowSet, chunkSize);
                final CloseableIterator<RowSequence> dstIt = rowSequenceIterator(dstRowSet, chunkSize)) {
            int bufferLen = 0;
            WritableObjectChunk<T, Values> buffer = buffer1;
            WritableObjectChunk<T, Values> readyBuffer = null;

            RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            RowSetBuilderSequential readyBuilder = null;

            while (srcIt.hasNext() && dstIt.hasNext()) {
                final int size;
                {
                    final RowSequence srcSeq = srcIt.next();
                    final RowSequence dstSeq = dstIt.next();
                    srcColumnSource.fillPrevChunk(context, prev, srcSeq);
                    srcColumnSource.fillChunk(context, curr, srcSeq);
                    dstSeq.fillRowKeyChunk(dstKeys);
                    size = srcSeq.intSize();
                }
                for (int i = 0; i < size; ++i) {
                    final T t = curr.get(i);
                    if (includePredicate.test(prev.get(i), t)) {
                        buffer.set(bufferLen++, t);
                        builder.appendKey(dstKeys.get(i));
                        if (bufferLen == chunkSize) {
                            readyBuffer = buffer;
                            readyBuffer.setSize(bufferLen);
                            readyBuilder = builder;
                            buffer = buffer == buffer1 ? buffer2 : buffer1;
                            bufferLen = 0;
                            builder = RowSetFactory.builderSequential();
                        }
                    }
                }
                if (readyBuffer != null) {
                    processed = true;
                    doProcess(fillContexts, processor, dstColumnSources, intermediates, readyBuffer, readyBuilder);
                    readyBuffer = null;
                    readyBuilder = null;
                }
            }
            if (srcIt.hasNext() || dstIt.hasNext()) {
                throw new IllegalStateException();
            }
            if (bufferLen != 0) {
                processed = true;
                buffer.setSize(bufferLen);
                doProcess(fillContexts, processor, dstColumnSources, intermediates, buffer, builder);
            }
        } finally {
            SafeCloseable.closeAll(Stream.concat(
                    intermediates.stream(),
                    Arrays.stream(fillContexts)));
        }
        return processed;
    }

    private static <T> void doProcess(
            FillFromContext[] fillContexts,
            ObjectProcessor<? super T> processor,
            Collection<WritableColumnSource<?>> dstColumnSources,
            List<WritableChunk<?>> intermediates,
            WritableObjectChunk<T, Values> srcChunk,
            RowSetBuilderSequential dstBuilder) {
        for (final WritableChunk<?> intermediate : intermediates) {
            intermediate.setSize(0);
        }
        processor.processAll(srcChunk, intermediates);
        srcChunk.fillWithNullValue(0, srcChunk.size());
        int i = 0;
        try (final WritableRowSet dstRS = dstBuilder.build()) {
            for (final WritableColumnSource<?> dstColumnSource : dstColumnSources) {
                // noinspection unchecked
                dstColumnSource.fillFromChunk(fillContexts[i], (Chunk<? extends Values>) intermediates.get(i), dstRS);
                ++i;
            }
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
