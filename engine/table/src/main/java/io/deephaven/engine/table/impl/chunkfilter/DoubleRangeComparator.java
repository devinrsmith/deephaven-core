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
    public static ChunkFilter makeDoubleFilter(double lower, double upper, boolean lowerInclusive, boolean upperInclusive) {
        return makeDoubleFilter(false, lower, upper, lowerInclusive, upperInclusive);
    }

    public static ChunkFilter makeDoubleFilter(boolean invertMatch, double lower, double upper, boolean lowerInclusive, boolean upperInclusive) {
        DoublePredicate p = DoubleComparisons.between(lower, upper, lowerInclusive, upperInclusive);
        if (invertMatch) {
            p = p.negate();
        }
        if (DoubleComparisons.isTrue(p)) {
            return ChunkFilter.TRUE_FILTER_INSTANCE;
        }
        if (DoubleComparisons.isFalse(p)) {
            return ChunkFilter.FALSE_FILTER_INSTANCE;
        }
        return new DoubleChunkFilterPredicateImpl(p);
    }

    private DoubleRangeComparator() {} // static use only
}
