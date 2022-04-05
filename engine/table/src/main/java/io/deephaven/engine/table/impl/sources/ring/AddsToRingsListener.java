package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SwapListener;
import io.deephaven.engine.updategraph.UpdateCommitter;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

final class AddsToRingsListener extends BaseTable.ListenerImpl {

    enum Init {
        NONE, FROM_PREVIOUS, FROM_CURRENT
    }

    static Table of(SwapListener swapListener, Table parent, int capacity, Init init) {
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
        final QueryTable result = new QueryTable(RowSetFactory.empty().toTracking(), ringMap) {
            {
                setRefreshing(true);
                setFlat();
                addParentReference(swapListener);
            }
        };
        final AddsToRingsListener listener = new AddsToRingsListener("todo", parent, result, sources, rings);
        listener.init(init);
        swapListener.setListenerAndResult(listener, result);
        return result;
    }

    private final ColumnSource<?>[] sources;
    private final RingColumnSource<?>[] rings;
    private final FillContext[] fillContexts;
    private final UpdateCommitter<AddsToRingsListener> prevFlusher;

    private AddsToRingsListener(
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
        final int capacity = rings[0].capacity();
        for (RingColumnSource<?> ring : rings) {
            if (ring.capacity() != capacity) {
                throw new IllegalArgumentException();
            }
        }
        fillContexts = Arrays.stream(sources).map(s -> s.makeFillContext(capacity)).toArray(FillContext[]::new);
        prevFlusher = new UpdateCommitter<>(this, AddsToRingsListener::bringPreviousUpToDate);
    }

    private WritableRowSet resultRowSet() {
        return (WritableRowSet) getDependent().getRowSet();
    }

    private void init(Init init) {
        if (init == Init.NONE) {
            return;
        }
        final boolean usePrev = init == Init.FROM_PREVIOUS;
        final long size;
        final TrackingRowSet parentRowSet = getParent().getRowSet();
        try (final RowSet srcKeys = usePrev ? parentRowSet.copyPrev() : parentRowSet.copy()) {
            for (int i = 0; i < rings.length; ++i) {
                final ChunkSource<? extends Values> source = usePrev ? sources[i].getPrevSource() : sources[i];
                // todo: can I use this fill context w/ prev source?
                rings[i].append(fillContexts[i], source, srcKeys);
                rings[i].bringPreviousUpToDate(fillContexts[i]);
            }
            size = srcKeys.size();
        }
        resultRowSet().insertRange(0, size - 1);
    }

    @Override
    public void onUpdate(TableUpdate upstream) {
        if (upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
            throw new IllegalStateException("Not expecting modifies or shifts");
        }
        // Ignoring any removes
        if (upstream.added().isEmpty()) {
            return;
        }
        append(upstream.added());
    }

    private void append(RowSet added) {
        for (int i = 0; i < rings.length; ++i) {
            rings[i].append(fillContexts[i], sources[i], added);
        }
        prevFlusher.maybeActivate();
        final TableUpdate update = rings[0].tableUpdate();
        resultRowSet().update(update.added(), update.removed());
        getDependent().notifyListeners(update);
    }

    private void bringPreviousUpToDate() {
        for (int i = 0; i < rings.length; i++) {
            // todo: is this the appropriate fill context to use? (coming from source)
            rings[i].bringPreviousUpToDate(fillContexts[i]);
        }
    }
}
