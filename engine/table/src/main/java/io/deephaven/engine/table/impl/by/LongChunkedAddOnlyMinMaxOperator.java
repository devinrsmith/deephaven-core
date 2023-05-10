/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkedAddOnlyMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by;

import java.time.Instant;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.DateTimeArraySource;
import io.deephaven.engine.table.impl.sources.InstantArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.NanosBasedTimeArraySource;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class LongChunkedAddOnlyMinMaxOperator implements IterativeChunkedAggregationOperator {
    private final LongArraySource resultColumn;
    // region actualResult
    private final ColumnSource<?> actualResult;
    // endregion actualResult
    private final boolean minimum;
    private final String name;

    LongChunkedAddOnlyMinMaxOperator(
            // region extra constructor params
            Class<?> type,
            // endregion extra constructor params
            boolean minimum, String name) {
        this.minimum = minimum;
        this.name = name;
        // region resultColumn initialization
        if (type == DateTime.class) {
            actualResult = new DateTimeArraySource();
            resultColumn = ((NanosBasedTimeArraySource<?>)actualResult).toEpochNano();
        } else if (type == Instant.class) {
            actualResult = new InstantArraySource();
            resultColumn = ((NanosBasedTimeArraySource<?>)actualResult).toEpochNano();
        } else {
            actualResult = resultColumn = new LongArraySource();
        }
        // endregion resultColumn initialization
    }

    private long min(LongChunk<?> values, MutableBoolean hasValue, int chunkStart, int chunkEnd) {
        long value = QueryConstants.MAX_LONG;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final long candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_LONG) {
                hasValue.setTrue();
                value = LongComparisons.min(value, candidate);
                if (value == QueryConstants.MIN_LONG) {
                    break;
                }
            }
        }
        return value;
    }

    private long max(LongChunk<?> values, MutableBoolean hasValue, int chunkStart, int chunkEnd) {
        long value = QueryConstants.MIN_LONG;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final long candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_LONG) {
                hasValue.setTrue();
            }
            value = LongComparisons.max(value, candidate);
            if (value == QueryConstants.MAX_LONG) {
                break;
            }
        }
        return value;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final LongChunk<? extends Values> asLongChunk = values.asLongChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asLongChunk, destination, startPosition, length.get(ii)));
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
        return addChunk(values.asLongChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        throw new UnsupportedOperationException();
    }

    private boolean addChunk(LongChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        if (chunkSize == 0) {
            return false;
        }
        final long oldValue = resultColumn.getUnsafe(destination);
        if ((minimum && oldValue == QueryConstants.MIN_LONG) || (!minimum && oldValue == QueryConstants.MAX_LONG)) {
            return false;
        }

        final MutableBoolean hasValue = new MutableBoolean(false);
        final int chunkEnd = chunkStart + chunkSize;
        final long chunkValue = minimum ? min(values, hasValue, chunkStart, chunkEnd) : max(values, hasValue, chunkStart, chunkEnd);
        if (hasValue.isFalse()) {
            return false;
        }

        final long result;
        if (oldValue == QueryConstants.NULL_LONG) {
            // we exclude nulls from the min/max calculation, therefore if the value in our min/max is null we know
            // that it is in fact empty and we should use the value from the chunk
            result = chunkValue;
        } else {
            result = minimum ? LongComparisons.min(chunkValue, oldValue) : LongComparisons.max(chunkValue, oldValue);
        }
        if (!LongComparisons.eq(result, oldValue)) {
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
        return Collections.<String, ColumnSource<?>>singletonMap(name, actualResult);
        // endregion getResultColumns
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }
}
