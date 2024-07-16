//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.compare.ByteComparisons;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.util.compare.IntComparisons;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.util.compare.ShortComparisons;

import java.util.function.Predicate;

import static io.deephaven.util.QueryConstants.*;

public class ColumnComparatorFactory {
    /**
     * Produce an {@link IComparator} specialized for a given left and right {@link ColumnSource}. To do this we look at
     * the underlying types of the column sources (actually we require those underlying types to be the same) and we use
     * this information to call the specific primitive type getter method (whether {@link ColumnSource#getDouble},
     * {@link ColumnSource#getLong} and so on). This approach allows us to avoid boxing on these calls. We use a similar
     * approach in order to test the null-ness of a given element.
     *
     * This method is not especially efficient, but (because we are typically not being called from an inner loop), that
     * fact is probably not relevant for performance. The point is that the returned IComparator *is* rather efficient.
     * 
     * @param lcs The left-hand ColumnSource (uses current values)
     * @param rcs The right-hand ColumnSource (uses current values)
     * @return An AbstractColumnSource.IComparator designed to compare elements from the two column sources.
     */
    public static IComparator createComparator(final ColumnSource lcs,
            final ColumnSource rcs) {
        final Class lType = lcs.getType();
        final Class rType = rcs.getType();
        Assert.eq(lType, "lType", rType, "rType");

        // lType equals rType, so we could use either here.
        final Predicate<Class> provides = lType::equals;

        if (provides.test(byte.class)) {
            return (lKey, rKey) -> ByteComparisons.compare(lcs.getByte(lKey), rcs.getByte(rKey));
        }
        if (provides.test(char.class)) {
            return (lKey, rKey) -> CharComparisons.compare(lcs.getChar(lKey), rcs.getChar(rKey));
        }
        if (provides.test(short.class)) {
            return (lKey, rKey) -> ShortComparisons.compare(lcs.getShort(lKey), rcs.getShort(rKey));
        }
        if (provides.test(int.class)) {
            return (lKey, rKey) -> IntComparisons.compare(lcs.getInt(lKey), rcs.getInt(rKey));
        }
        if (provides.test(long.class)) {
            return (lKey, rKey) -> LongComparisons.compare(lcs.getLong(lKey), rcs.getLong(rKey));
        }
        if (provides.test(float.class)) {
            return (lKey, rKey) -> FloatComparisons.compare(lcs.getFloat(lKey), rcs.getFloat(rKey));
        }
        if (provides.test(double.class)) {
            return (lKey, rKey) -> DoubleComparisons.compare(lcs.getDouble(lKey), rcs.getDouble(rKey));
        }
        // fall through to Object interface
        return (lKey, rKey) -> ObjectComparisons.compare(lcs.get(lKey), rcs.get(rKey));
    }

    /**
     * Produce an {@link IComparator} specialized for a given left and right {@link ColumnSource}. To do this we look at
     * the underlying types of the column sources (actually we require those underlying types to be the same) and we use
     * this information to call the specific primitive type getter method (whether {@link ColumnSource#getDouble},
     * {@link ColumnSource#getLong} and so on). This approach allows us to avoid boxing on these calls. We use a similar
     * approach in order to test the null-ness of a given element.
     *
     * This method is not especially efficient, but (because we are typically not being called from an inner loop), that
     * fact is probably not relevant for performance. The point is that the returned IComparatorEnhanced *is* rather
     * efficient.
     * 
     * @param lcs The left-hand ColumnSource (uses current values)
     * @param rcs The right-hand ColumnSource (uses previous values)
     * @return An AbstractColumnSource.IComparator designed to compare elements from the two column sources.
     */
    public static IComparator createComparatorLeftCurrRightPrev(
            final ColumnSource lcs, final ColumnSource rcs) {
        final Class lType = lcs.getType();
        final Class rType = rcs.getType();
        Assert.eq(lType, "lType", rType, "rType");

        // lType equals rType, so we could use either here.
        final Predicate<Class> provides = lType::equals;

        if (provides.test(byte.class)) {
            return (lKey, rKey) -> ByteComparisons.compare(lcs.getByte(lKey), rcs.getPrevByte(rKey));
        }
        if (provides.test(char.class)) {
            return (lKey, rKey) -> CharComparisons.compare(lcs.getChar(lKey), rcs.getPrevChar(rKey));
        }
        if (provides.test(short.class)) {
            return (lKey, rKey) -> ShortComparisons.compare(lcs.getShort(lKey), rcs.getPrevShort(rKey));
        }
        if (provides.test(int.class)) {
            return (lKey, rKey) -> IntComparisons.compare(lcs.getInt(lKey), rcs.getPrevInt(rKey));
        }
        if (provides.test(long.class)) {
            return (lKey, rKey) -> LongComparisons.compare(lcs.getLong(lKey), rcs.getPrevLong(rKey));
        }
        if (provides.test(float.class)) {
            return (lKey, rKey) -> FloatComparisons.compare(lcs.getFloat(lKey), rcs.getPrevFloat(rKey));
        }
        if (provides.test(double.class)) {
            return (lKey, rKey) -> DoubleComparisons.compare(lcs.getDouble(lKey), rcs.getPrevDouble(rKey));
        }
        // fall through to Object interface
        return (lKey, rKey) -> ObjectComparisons.compare(lcs.get(lKey), rcs.getPrev(rKey));
    }

    public interface IComparator {
        int compare(long pos1, long pos2);
    }
}
