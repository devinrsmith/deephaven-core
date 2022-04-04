package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ChunkSource.GetContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.ring.RingColumnSource;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Tools for manipulating tables.
 */
public class StreamTableTools {
    /**
     * Convert a Stream Table to an in-memory append only table.
     *
     * Note, this table will grow without bound as new stream values are encountered.
     *
     * @param streamTable the input stream table
     * @return an append-only in-memory table representing all data encountered in the stream
     */
    public static Table streamToAppendOnlyTable(final Table streamTable) {
        return QueryPerformanceRecorder.withNugget("streamToAppendOnlyTable", () -> {
            if (!isStream(streamTable)) {
                throw new IllegalArgumentException("Input is not a stream table!");
            }

            final BaseTable baseStreamTable = (BaseTable) streamTable.coalesce();

            final SwapListener swapListener =
                    baseStreamTable.createSwapListenerIfRefreshing(SwapListener::new);
            // stream tables must tick
            Assert.neqNull(swapListener, "swapListener");

            final Mutable<QueryTable> resultHolder = new MutableObject<>();

            ConstructSnapshot.callDataSnapshotFunction("streamToAppendOnlyTable", swapListener.makeSnapshotControl(),
                    (boolean usePrev, long beforeClockValue) -> {
                        final Map<String, ArrayBackedColumnSource<?>> columns = new LinkedHashMap<>();
                        final Map<String, ? extends ColumnSource<?>> columnSourceMap = streamTable.getColumnSourceMap();
                        final int columnCount = columnSourceMap.size();
                        final ColumnSource<?>[] sourceColumns = new ColumnSource[columnCount];
                        final WritableColumnSource<?>[] destColumns = new WritableColumnSource[columnCount];
                        int colIdx = 0;
                        for (Map.Entry<String, ? extends ColumnSource<?>> nameColumnSourceEntry : columnSourceMap
                                .entrySet()) {
                            final ColumnSource<?> existingColumn = nameColumnSourceEntry.getValue();
                            final ArrayBackedColumnSource<?> newColumn = ArrayBackedColumnSource.getMemoryColumnSource(
                                    0, existingColumn.getType(), existingColumn.getComponentType());
                            columns.put(nameColumnSourceEntry.getKey(), newColumn);
                            // for the source columns, we would like to read primitives instead of objects in cases
                            // where it is possible
                            sourceColumns[colIdx] = ReinterpretUtils.maybeConvertToPrimitive(existingColumn);
                            // for the destination sources, we know they are array backed sources that will actually
                            // store primitives and we can fill efficiently
                            destColumns[colIdx++] =
                                    (WritableColumnSource<?>) ReinterpretUtils.maybeConvertToPrimitive(newColumn);
                        }


                        final TrackingWritableRowSet rowSet;
                        if (usePrev) {
                            try (final RowSet useRowSet = baseStreamTable.getRowSet().copyPrev()) {
                                rowSet = RowSetFactory.flat(useRowSet.size()).toTracking();
                                ChunkUtils.copyData(sourceColumns, useRowSet, destColumns, rowSet, usePrev);
                            }
                        } else {
                            rowSet = RowSetFactory.flat(baseStreamTable.getRowSet().size())
                                    .toTracking();
                            ChunkUtils.copyData(sourceColumns, baseStreamTable.getRowSet(), destColumns, rowSet,
                                    usePrev);
                        }

                        final QueryTable result = new QueryTable(rowSet, columns);
                        result.setRefreshing(true);
                        result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
                        result.setFlat();
                        result.addParentReference(swapListener);
                        resultHolder.setValue(result);

                        swapListener.setListenerAndResult(new BaseTable.ListenerImpl("streamToAppendOnly",
                                streamTable, result) {
                            @Override
                            public void onUpdate(TableUpdate upstream) {
                                if (upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
                                    throw new IllegalStateException("Stream tables should not modify or shift!");
                                }
                                if (upstream.added().isEmpty()) {
                                    return;
                                }
                                final long newRows = upstream.added().size();
                                final long currentSize = rowSet.size();
                                columns.values().forEach(c -> c.ensureCapacity(currentSize + newRows));

                                final RowSet newRange =
                                        RowSetFactory.fromRange(currentSize,
                                                currentSize + newRows - 1);

                                ChunkUtils.copyData(sourceColumns, upstream.added(), destColumns, newRange, false);
                                rowSet.insertRange(currentSize, currentSize + newRows - 1);

                                final TableUpdateImpl downstream = new TableUpdateImpl();
                                downstream.added = newRange;
                                downstream.modified = RowSetFactory.empty();
                                downstream.removed = RowSetFactory.empty();
                                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                                downstream.shifted = RowSetShiftData.EMPTY;
                                result.notifyListeners(downstream);
                            }
                        }, result);

                        return true;
                    });

            return resultHolder.getValue();
        });
    }

    public static Table streamToRingTable(final Table streamTable, final int capacity) {
        return QueryPerformanceRecorder.withNugget("streamToRingTable", () -> {
            if (!isStream(streamTable)) {
                throw new IllegalArgumentException("Input is not a stream table!");
            }

            final BaseTable baseStreamTable = (BaseTable) streamTable.coalesce();

            final SwapListener swapListener =
                    baseStreamTable.createSwapListenerIfRefreshing(SwapListener::new);
            // stream tables must tick
            Assert.neqNull(swapListener, "swapListener");

            final Mutable<QueryTable> resultHolder = new MutableObject<>();

            ConstructSnapshot.callDataSnapshotFunction("streamToRingTable", swapListener.makeSnapshotControl(),
                    (boolean usePrev, long beforeClockValue) -> {
                        final Map<String, ArrayBackedColumnSource<?>> columns = new LinkedHashMap<>();
                        final Map<String, ? extends ColumnSource<?>> columnSourceMap = streamTable.getColumnSourceMap();
                        final int columnCount = columnSourceMap.size();
                        final ColumnSource<?>[] sourceColumns = new ColumnSource[columnCount];
                        final WritableColumnSource<?>[] destColumns = new WritableColumnSource[columnCount];
                        int colIdx = 0;
                        for (Map.Entry<String, ? extends ColumnSource<?>> nameColumnSourceEntry : columnSourceMap
                                .entrySet()) {
                            final ColumnSource<?> existingColumn = nameColumnSourceEntry.getValue();
                            final ArrayBackedColumnSource<?> newColumn = ArrayBackedColumnSource.getMemoryColumnSource(
                                    0, existingColumn.getType(), existingColumn.getComponentType());
                            columns.put(nameColumnSourceEntry.getKey(), newColumn);
                            // for the source columns, we would like to read primitives instead of objects in cases
                            // where it is possible
                            sourceColumns[colIdx] = ReinterpretUtils.maybeConvertToPrimitive(existingColumn);
                            // for the destination sources, we know they are array backed sources that will actually
                            // store primitives and we can fill efficiently
                            destColumns[colIdx++] =
                                    (WritableColumnSource<?>) ReinterpretUtils.maybeConvertToPrimitive(newColumn);
                        }


                        final TrackingWritableRowSet rowSet;
                        if (usePrev) {
                            try (final RowSet useRowSet = baseStreamTable.getRowSet().copyPrev()) {
                                rowSet = RowSetFactory.flat(useRowSet.size()).toTracking();
                                ChunkUtils.copyData(sourceColumns, useRowSet, destColumns, rowSet, usePrev);
                            }
                        } else {
                            rowSet = RowSetFactory.flat(baseStreamTable.getRowSet().size())
                                    .toTracking();
                            ChunkUtils.copyData(sourceColumns, baseStreamTable.getRowSet(), destColumns, rowSet,
                                    usePrev);
                        }

                        final QueryTable result = new QueryTable(rowSet, columns);
                        result.setRefreshing(true);
                        result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
                        result.setFlat();
                        result.addParentReference(swapListener);
                        resultHolder.setValue(result);

                        swapListener.setListenerAndResult(ContiguousAddsToRingTableListener.of(streamTable, capacity), result);

                        return true;
                    });

            return resultHolder.getValue();
        });
    }

    /**
     * Returns true if table is a stream table.
     *
     * @param table the table to check for stream behavior
     * @return Whether this table is a stream table
     * @see Table#STREAM_TABLE_ATTRIBUTE
     */
    public static boolean isStream(Table table) {
        if (!table.isRefreshing()) {
            return false;
        }
        return Boolean.TRUE.equals(table.getAttribute(Table.STREAM_TABLE_ATTRIBUTE));
    }

    private static class ContiguousAddsToRingTableListener extends BaseTable.ListenerImpl {

        public static Table of(Table parent, int capacity) {
            final Map<String, ? extends ColumnSource<?>> sourceMap = parent.getColumnSourceMap();
            final int numColumns = sourceMap.size();
            final Map<String, RingColumnSource<?>> ringMap = new LinkedHashMap<>(numColumns);
            final ColumnSource<?>[] sources = new ColumnSource[numColumns];
            final RingColumnSource<?>[] rings = new RingColumnSource[numColumns];
            int ix = 0;
            for (Map.Entry<String, ? extends ColumnSource<?>> e : sourceMap.entrySet()) {
                final String name = e.getKey();
                final ColumnSource<?> source = e.getValue();
                final RingColumnSource<?> ring = RingColumnSource.of(capacity, source.getType(), source.getComponentType());
                sources[ix] = source;
                rings[ix] = ring;
                ringMap.put(name, ring);
                ++ix;
            }
            final WritableRowSet rowSet = null;
            final ContiguousAddsToRingTableListener listener =
                    new ContiguousAddsToRingTableListener("todo", parent, null, rowSet, sources, rings);

        }

        private final WritableRowSet rowSet;
        private final ColumnSource<?>[] sources;
        private final RingColumnSource<?>[] rings;

        private ContiguousAddsToRingTableListener(
                String description,
                Table parent,
                BaseTable dependent,
                WritableRowSet rowSet,
                ColumnSource<?>[] sources,
                RingColumnSource<?>[] rings) {
            super(description, parent, dependent);

            this.sources = Objects.requireNonNull(sources);
            this.rings = Objects.requireNonNull(rings);
            if (sources.length != rings.length) {
                throw new IllegalArgumentException();
            }
            if (sources.length == 0) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            if (upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
                throw new IllegalStateException("Not expecting modifies or shifts");
            }
            final RowSet added = upstream.added();
            if (!added.isContiguous()) {
                throw new IllegalStateException("Expected added row set to be contiguous");
            }
            if (added.isEmpty()) {
                return;
            }
            // todo: we know the max capacity
            final FillContext fillContext = null;
            final GetContext context = null;

            final long firstKey = added.firstRowKey();
            final long lastKey = added.lastRowKey();
            for (int i = 0; i < sources.length; ++i) {
                //noinspection unchecked,rawtypes
                rings[i].append((ColumnSource) sources[i], fillContext, context, firstKey, lastKey);
            }
            rings[0].updateTracking(rowSet);
            for (RingColumnSource<?> ring : rings) {
                ring.copyCurrentToPrevious(fillContext, context);
            }
        }
    }
}
