/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatRangeComparator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.DoubleComparisons;

import java.util.function.DoublePredicate;

public class DoubleRangeComparator {
    public static ChunkFilter.DoubleChunkFilter makeDoubleFilter(double lower, double upper, boolean lowerInclusive, boolean upperInclusive) {
        return makeDoubleFilter(false, lower, upper, lowerInclusive, upperInclusive);
    }

    public static ChunkFilter.DoubleChunkFilter makeDoubleFilter(boolean invertMatch, double lower, double upper, boolean lowerInclusive, boolean upperInclusive) {
        final DoublePredicate lowerPredicate = lowerInclusive
                ? DoubleComparisons.geq(lower)
                : DoubleComparisons.gt(lower);
        final DoublePredicate upperPredicate = upperInclusive
                ? DoubleComparisons.leq(upper)
                : DoubleComparisons.lt(upper);
        final DoublePredicate predicate = invertMatch
                ? lowerPredicate.negate().or(upperPredicate.negate())
                : lowerPredicate.and(upperPredicate);
        return new DoubleChunkFilterPredicateImpl(predicate);
    }

    private DoubleRangeComparator() {} // static use only
}
