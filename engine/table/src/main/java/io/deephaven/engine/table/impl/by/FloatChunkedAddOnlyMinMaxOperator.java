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
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class FloatChunkedAddOnlyMinMaxOperator implements IterativeChunkedAggregationOperator {
    private final FloatArraySource resultColumn;
    // region actualResult
    // endregion actualResult
    private final boolean minimum;
    private final String name;

    FloatChunkedAddOnlyMinMaxOperator(
            // region extra constructor params
            // endregion extra constructor params
            boolean minimum, String name) {
        this.minimum = minimum;
        this.name = name;
        // region resultColumn initialization
        this.resultColumn = new FloatArraySource();
        // endregion resultColumn initialization
    }

    private float min(FloatChunk<?> values, MutableBoolean hasValue, int chunkStart, int chunkEnd) {
        float value = QueryConstants.MAX_FLOAT;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final float candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_FLOAT) {
                hasValue.setTrue();
                value = FloatComparisons.min(value, candidate);
                if (value == QueryConstants.MIN_FLOAT) {
                    break;
                }
            }
        }
        return value;
    }

    private float max(FloatChunk<?> values, MutableBoolean hasValue, int chunkStart, int chunkEnd) {
        float value = QueryConstants.MIN_FLOAT;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final float candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_FLOAT) {
                hasValue.setTrue();
            }
            value = FloatComparisons.max(value, candidate);
            if (value == QueryConstants.MAX_FLOAT) {
                break;
            }
        }
        return value;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final FloatChunk<? extends Values> asFloatChunk = values.asFloatChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asFloatChunk, destination, startPosition, length.get(ii)));
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
        return addChunk(values.asFloatChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        throw new UnsupportedOperationException();
    }

    private boolean addChunk(FloatChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        if (chunkSize == 0) {
            return false;
        }
        final float oldValue = resultColumn.getUnsafe(destination);
        if ((minimum && oldValue == QueryConstants.MIN_FLOAT) || (!minimum && oldValue == QueryConstants.MAX_FLOAT)) {
            return false;
        }

        final MutableBoolean hasValue = new MutableBoolean(false);
        final int chunkEnd = chunkStart + chunkSize;
        final float chunkValue = minimum ? min(values, hasValue, chunkStart, chunkEnd) : max(values, hasValue, chunkStart, chunkEnd);
        if (hasValue.isFalse()) {
            return false;
        }

        final float result;
        if (oldValue == QueryConstants.NULL_FLOAT) {
            // we exclude nulls from the min/max calculation, therefore if the value in our min/max is null we know
            // that it is in fact empty and we should use the value from the chunk
            result = chunkValue;
        } else {
            result = minimum ? FloatComparisons.min(chunkValue, oldValue) : FloatComparisons.max(chunkValue, oldValue);
        }
        if (!FloatComparisons.eq(result, oldValue)) {
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
