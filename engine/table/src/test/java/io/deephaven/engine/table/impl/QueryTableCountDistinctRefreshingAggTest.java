//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

public class QueryTableCountDistinctRefreshingAggTest extends QueryTableCountDistinctAggTestBase {
    @Override
    public Table countDistinct(boolean includeNullAndNans, char[] source) {
        final Table x = TableTools.newTable(TableTools.charCol(S1, source));
        x.setRefreshing(true);
        return x.aggAllBy(AggSpec.countDistinct(includeNullAndNans));
    }

    @Override
    public Table countDistinct(boolean includeNullAndNans, float[] source) {
        final Table x = TableTools.newTable(TableTools.floatCol(S1, source));
        x.setRefreshing(true);
        return x.aggAllBy(AggSpec.countDistinct(includeNullAndNans));
    }

    @Override
    public Table countDistinct(boolean includeNullAndNans, double[] source) {
        final Table x = TableTools.newTable(TableTools.doubleCol(S1, source));
        x.setRefreshing(true);
        return x.aggAllBy(AggSpec.countDistinct(includeNullAndNans));
    }
}
