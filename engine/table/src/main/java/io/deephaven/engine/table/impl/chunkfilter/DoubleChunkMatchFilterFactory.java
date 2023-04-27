/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkMatchFilterFactory and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.iterator.TDoubleIterator;
import gnu.trove.set.hash.TDoubleHashSet;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter.DoubleChunkFilter;

import java.util.Objects;
import java.util.function.DoublePredicate;

/**
 * Creates chunk filters for double values.
 *
 * <p>
 * The strategy is that for one, two, or three values we have specialized
 * classes that will do the appropriate simple equality check.
 *
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class DoubleChunkMatchFilterFactory {
    private DoubleChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.DoubleChunkFilter makeFilter(boolean invertMatch, double... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException();
        }
        final DoublePredicate predicate = predicate(values);
        return new DoubleChunkFilterPredicateImpl(invertMatch ? predicate.negate() : predicate);
    }

    private static DoublePredicate predicate(double... values) {
        boolean matchNaN = false;
        final TDoubleHashSet set = new TDoubleHashSet(values.length);
        for (double value : values) {
            if (Double.isNaN(value)) {
                matchNaN = true;
                continue;
            }
            set.add(value);
        }
        if (set.isEmpty()) {
            if (!matchNaN) {
                throw new IllegalStateException();
            }
            return Double::isNaN;
        }
        final DoublePredicate p1 = predicate(set);
        return matchNaN ? ((DoublePredicate) Double::isNaN).or(p1) : p1;
    }

    private static DoublePredicate predicate(TDoubleHashSet set) {
        if (set.isEmpty()) {
            throw new IllegalArgumentException();
        }
        final TDoubleIterator it = set.iterator();
        final double value1 = it.next();
        if (!it.hasNext()) {
            return new SingleValue(value1);
        }
        final double value2 = it.next();
        if (!it.hasNext()) {
            return new TwoValues(value1, value2);
        }
        final double value3 = it.next();
        if (!it.hasNext()) {
            return new ThreeValues(value1, value2, value3);
        }
        return new MultiValues(set);
    }

    private static final class SingleValue implements DoublePredicate {
        private final double value1;

        public SingleValue(double value1) {
            this.value1 = value1;
        }

        @Override
        public boolean test(double value) {
            return value == value1;
        }
    }

    private static final class TwoValues implements DoublePredicate {
        private final double value1;
        private final double value2;

        public TwoValues(double value1, double value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean test(double value) {
            return value == value1 || value == value2;
        }
    }

    private static final class ThreeValues implements DoublePredicate {
        private final double value1;
        private final double value2;
        private final double value3;

        public ThreeValues(double value1, double value2, double value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean test(double value) {
            return value == value1 || value == value2 || value == value3;
        }
    }

    private static final class MultiValues implements DoublePredicate {
        private final TDoubleHashSet set;

        public MultiValues(TDoubleHashSet set) {
            this.set = Objects.requireNonNull(set);
        }

        @Override
        public boolean test(double value) {
            return set.contains(value);
        }
    }

    private static final class DoubleChunkFilterPredicateImpl implements DoubleChunkFilter {
        private final DoublePredicate predicate;

        public DoubleChunkFilterPredicateImpl(DoublePredicate predicate) {
            this.predicate = Objects.requireNonNull(predicate);
        }

        @Override
        public void filter(
                DoubleChunk<? extends Values> values,
                LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (predicate.test(values.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }
}