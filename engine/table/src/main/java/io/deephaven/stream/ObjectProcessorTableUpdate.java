/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.stream;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.table.ChunkSink.FillFromContext;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.BaseTable.ListenerImpl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.util.SafeCloseable;

import java.util.List;

final class ObjectProcessorTableUpdate<T> extends ListenerImpl {

    private final Table source;
    private final String columnName;
    private final Class<T> columnType;
    private final ObjectProcessor<T> processor;
    private final WritableColumnSource<?>[] outputs;
    private final ModifiedColumnSet interest;
    private final ColumnSource<T> columnSource;

    private final int chunkSize;
    private final List<WritableChunk<Values>> cache;
    //         final List<WritableChunk<Values>> out = processor.outputTypes()
    //                .stream()
    //                .map(ObjectProcessor::chunkType)
    //                .map(o -> o.<Values>makeWritableChunk(chunkSize))
    //                .collect(Collectors.toList());


    @Override
    public void onUpdate(TableUpdate upstream) {
        if (!needsCalc(upstream)) {
            super.onUpdate(upstream);
            return;
        }

//        if (upstream.removed().isNonempty()) {
//            outputs[0].setNull(upstream.removed());
//        }

        if (upstream.added().isNonempty()) {

            columnSource.
        }

    }

    void handleAdds(TableUpdate upstream) {
        if (upstream.added().isEmpty()) {
            return;
        }
        final int chunkSize = 1024;
        // todo: we could save this

        try (
                final WritableObjectChunk<T, Values> src = WritableObjectChunk.makeWritableChunk(chunkSize);
                final FillContext context = columnSource.makeFillContext(chunkSize);
                final Iterator it = upstream.added().getRowSequenceIterator()) {
            while (it.hasMore()) {
                final RowSequence rowSeq = it.getNextRowSequenceWithLength(chunkSize);
                final int rowSeqSize = rowSeq.intSize();

                // Read into src
                columnSource.fillChunk(context, src, rowSeq);

                // Process into cache
                for (WritableChunk<Values> o : cache) {
                    o.setSize(0);
                }
                processor.processAll(src, (List) cache);

                // Write cache to outputs
                for (int i = 0; i < outputs.length; ++i) {
                    try (final FillFromContext fillFromContext = outputs[i].makeFillFromContext(chunkSize)) {
                        outputs[i].fillFromChunk(fillFromContext, cache.get(i), rowSeq);
                        cache.get(i).fillWithNullValue(0, rowSeqSize);
                    }
                }
            }
        }
    }

    private boolean needsCalc(TableUpdate upstream) {
        return upstream.added().isNonempty()
                || upstream.removed().isNonempty()
                || upstream.shifted().nonempty()
                || (upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(interest));
    }

    public void what() {
        new QueryTable(null, null);
    }
}
