/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkedAddOnlyMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class ShortChunkedAddOnlyMinMaxOperator implements IterativeChunkedAggregationOperator {
    private final ShortArraySource resultColumn;
    // region actualResult
    // endregion actualResult
    private final boolean minimum;
    private final String name;

    ShortChunkedAddOnlyMinMaxOperator(
            // region extra constructor params
            // endregion extra constructor params
            boolean minimum, String name) {
        this.minimum = minimum;
        this.name = name;
        // region resultColumn initialization
        this.resultColumn = new ShortArraySource();
        // endregion resultColumn initialization
    }

    private short min(ShortChunk<?> values, MutableBoolean hasValue, int chunkStart, int chunkEnd) {
        short value = QueryConstants.MAX_SHORT;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final short candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_SHORT) {
                hasValue.setTrue();
                value = ShortComparisons.min(value, candidate);
                if (value == QueryConstants.MIN_SHORT) {
                    break;
                }
            }
        }
        return value;
    }

    private short max(ShortChunk<?> values, MutableBoolean hasValue, int chunkStart, int chunkEnd) {
        short value = QueryConstants.MIN_SHORT;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final short candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_SHORT) {
                hasValue.setTrue();
            }
            value = ShortComparisons.max(value, candidate);
            if (value == QueryConstants.MAX_SHORT) {
                break;
            }
        }
        return value;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ShortChunk<? extends Values> asShortChunk = values.asShortChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asShortChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asShortChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        throw new UnsupportedOperationException();
    }

    private boolean addChunk(ShortChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        if (chunkSize == 0) {
            return false;
        }
        final short oldValue = resultColumn.getUnsafe(destination);
        if ((minimum && oldValue == QueryConstants.MIN_SHORT) || (!minimum && oldValue == QueryConstants.MAX_SHORT)) {
            return false;
        }

        final MutableBoolean hasValue = new MutableBoolean(false);
        final int chunkEnd = chunkStart + chunkSize;
        final short chunkValue = minimum ? min(values, hasValue, chunkStart, chunkEnd) : max(values, hasValue, chunkStart, chunkEnd);
        if (hasValue.isFalse()) {
            return false;
        }

        final short result;
        if (oldValue == QueryConstants.NULL_SHORT) {
            // we exclude nulls from the min/max calculation, therefore if the value in our min/max is null we know
            // that it is in fact empty and we should use the value from the chunk
            result = chunkValue;
        } else {
            result = minimum ? ShortComparisons.min(chunkValue, oldValue) : ShortComparisons.max(chunkValue, oldValue);
        }
        if (!ShortComparisons.eq(result, oldValue)) {
            resultColumn.set(destination, result);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        // region getResultColumns
        return Collections.<String, ColumnSource<?>>singletonMap(name, resultColumn);
        // endregion getResultColumns
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }
}
