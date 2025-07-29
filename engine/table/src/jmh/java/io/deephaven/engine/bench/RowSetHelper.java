//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.bench;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

final class RowSetHelper {

    public static WritableRowSet createRowSet(
            final long firstRowKey,
            final long lastRowKey,
            final boolean firstKeySet,
            final IntSupplier zeroRunSampler,
            final IntSupplier oneRunSampler) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.setDomain(firstRowKey, lastRowKey);
        long key = firstRowKey;
        if (!firstKeySet) {
            key += sample(zeroRunSampler);
        }
        while (key <= lastRowKey) {
            final int oneRunLen = sample(oneRunSampler);
            builder.appendRange(key, Math.min(key + oneRunLen - 1, lastRowKey));
            key += oneRunLen + sample(zeroRunSampler);
        }
        return builder.build();
    }

    private static int sample(IntSupplier supplier) {
        int value = supplier.getAsInt();
        Assert.gtZero(value, "value");
        return value;
    }

    public static List<WritableRowSet> equalKeySplit(final WritableRowSet rowSet, final int n) {
        final long size = rowSet.size();
        final long sizePer = size / n;
        final long extra = size - sizePer * n;
        final List<WritableRowSet> out = new ArrayList<>(n);
        try (final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            Assert.eqTrue(it.hasMore(), "it.hasMore()");
            final WritableRowSet first = it.getNextRowSequenceWithLength(sizePer + extra).asRowSet().writableCast();
            Assert.eq(first.size(), "first.size()", sizePer + extra);
            out.add(first);
            for (int i = 1; i < n; ++i) {
                Assert.eqTrue(it.hasMore(), "it.hasMore()");
                final WritableRowSet rs = it.getNextRowSequenceWithLength(sizePer).asRowSet().writableCast();
                Assert.eq(rs.size(), "rs.size()", sizePer);
                out.add(rs);
            }
            Assert.eqFalse(it.hasMore(), "it.hasMore()");
        }
        return out;
    }
}
