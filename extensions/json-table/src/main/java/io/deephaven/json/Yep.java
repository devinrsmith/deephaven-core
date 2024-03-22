//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

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
import io.deephaven.util.SafeCloseable;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Yep {

    public static <T> void processAll(
            ColumnSource<? extends T> srcColumnSource,
            RowSet srcRowSet,
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
        final List<WritableChunk<Values>> intermediateChunks = processor.outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(o -> o.<Values>makeWritableChunk(chunkSize))
                .collect(Collectors.toList());
        try (
                final WritableObjectChunk<? extends T, Values> src = WritableObjectChunk.makeWritableChunk(1024);
                final FillContext srcFillContext = srcColumnSource.makeFillContext(chunkSize);
                final Iterator srcIt = srcRowSet.getRowSequenceIterator();
                final Iterator dstIt = dstRowSet.getRowSequenceIterator()) {
            while (srcIt.hasMore() && dstIt.hasMore()) {
                final RowSequence srcSeq = srcIt.getNextRowSequenceWithLength(chunkSize);
                final RowSequence dstSeq = dstIt.getNextRowSequenceWithLength(chunkSize);
                final int rowSeqSize = srcSeq.intSize();
                if (dstSeq.intSize() != rowSeqSize) {
                    throw new IllegalStateException();
                }
                srcColumnSource.fillChunk(srcFillContext, src, srcSeq);
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
            if (srcIt.hasMore() || dstIt.hasMore()) {
                throw new IllegalStateException();
            }
        } finally {
            SafeCloseable.closeAll(Stream.concat(intermediateChunks.stream(), Stream.of(fillFromContexts)));
        }
    }
}
