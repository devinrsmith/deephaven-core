package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ChunkSource.GetContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SwapListener;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.UnionSourceManager;
import io.deephaven.engine.updategraph.UpdateCommitter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class ContiguousAddsToRingTableListener extends BaseTable.ListenerImpl {

    public static Table of(Table parent, int capacity) {
        return QueryPerformanceRecorder.withNugget("ContiguousAddsToRingTableListener.of", () -> {
            final BaseTable baseTable = (BaseTable) parent.coalesce();
            final SwapListener swapListener = baseTable.createSwapListenerIfRefreshing(SwapListener::new);
            Assert.neqNull(swapListener, "swapListener");
            final Table[] results = new Table[1];
            ConstructSnapshot.callDataSnapshotFunction("ContiguousAddsToRingTableListener.of", swapListener.makeSnapshotControl(),
                    (boolean usePrev, long beforeClockValue) -> {
                        final Table table = of(swapListener, parent, capacity);
                        results[0] = table;
                        return true;
                    });

            return results[0];
        });
    }

    public static Table of(SwapListener swapListener, Table parent, int capacity) {
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
        final TrackingRowSet rowSet = RowSetFactory.empty().toTracking();
        final QueryTable result = new QueryTable(rowSet, ringMap);
        result.setRefreshing(true);
//        result.setFlat();
        result.addParentReference(swapListener);

        // todo: prev and initial data
        // if I take snapshot initially, need to copy it to prev

        final ContiguousAddsToRingTableListener listener = new ContiguousAddsToRingTableListener("todo", parent, result, sources, rings);
        swapListener.setListenerAndResult(listener, result);
        return result;
    }

    private final ColumnSource<?>[] sources;
    private final RingColumnSource<?>[] rings;
    private final UpdateCommitter<ContiguousAddsToRingTableListener> prevFlusher;

    private ContiguousAddsToRingTableListener(
            String description,
            Table parent,
            BaseTable dependent,
            ColumnSource<?>[] sources,
            RingColumnSource<?>[] rings) {
        super(description, parent, Objects.requireNonNull(dependent));
        this.sources = Objects.requireNonNull(sources);
        this.rings = Objects.requireNonNull(rings);
        if (sources.length != rings.length) {
            throw new IllegalArgumentException();
        }
        if (sources.length == 0) {
            throw new IllegalArgumentException();
        }
        if (!(dependent.getRowSet() instanceof WritableRowSet)) {
            throw new IllegalArgumentException("Expected writable row set");
        }
        prevFlusher = new UpdateCommitter<>(this, ContiguousAddsToRingTableListener::updatePrev);
    }

    @Override
    public void onUpdate(TableUpdate upstream) {
        if (upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
            throw new IllegalStateException("Not expecting modifies or shifts");
        }
        final RowSet upstreamAdded = upstream.added();
        if (!upstreamAdded.isContiguous()) {
            throw new IllegalStateException("Expected added row set to be contiguous");
        }

        if (upstreamAdded.isEmpty()) {
            return;
        }

        final long firstKey = upstreamAdded.firstRowKey();
        final long lastKey = upstreamAdded.lastRowKey();
        final int capacity = rings[0].capacity();

        for (int i = 0; i < sources.length; ++i) {
            // todo: do we have to do all this context creating?
            try (
                    final GetContext getContext = rings[i].makeGetContext(capacity);
                    final FillContext fillContext = sources[i].makeFillContext(capacity)) {
                //noinspection unchecked,rawtypes
                rings[i].append((ColumnSource) sources[i], fillContext, getContext, firstKey, lastKey);
            }
        }

        prevFlusher.maybeActivate();

        final TableUpdate downstreamUpdate = rings[0].tableUpdate();
        final BaseTable dependent = getDependent();
        ((WritableRowSet)dependent.getRowSet()).update(downstreamUpdate.added(), downstreamUpdate.removed());
        dependent.notifyListeners(downstreamUpdate);
    }

    private void updatePrev() {
        for (RingColumnSource<?> ring : rings) {
            try (
                    final GetContext getContext = ring.makeGetContext(rings[0].capacity());
                    final FillContext fillContext = ring.makeFillContext(rings[0].capacity())) {
                ring.copyCurrentToPrevious(fillContext, getContext);
            }
        }
    }
}
