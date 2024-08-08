//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import org.junit.Ignore;

// Does not support NaN
@Ignore
public class QueryTableDistinctGroupedFormulaAggTest extends QueryTableDistinctAggTestBase {
    @Override
    public Table distinct(boolean includeNullAndNans, char[] source) {
        final String forumla = includeNullAndNans ? "S1=distinct(S1, true)" : "S1=distinct(S1)";
        return TableTools.newTable(TableTools.charCol(S1, source)).groupBy().update(forumla).ungroup(S1);
    }

    @Override
    public Table distinct(boolean includeNullAndNans, float[] source) {
        final String forumla = includeNullAndNans ? "S1=distinct(S1, true)" : "S1=distinct(S1)";
        return TableTools.newTable(TableTools.floatCol(S1, source)).groupBy().update(forumla).ungroup(S1);
    }

    @Override
    public Table distinct(boolean includeNullAndNans, double[] source) {
        final String forumla = includeNullAndNans ? "S1=distinct(S1, true)" : "S1=distinct(S1)";
        return TableTools.newTable(TableTools.doubleCol(S1, source)).groupBy().update(forumla).ungroup(S1);
    }

    @Override
    public Table countDistinct(boolean includeNullAndNans, char[] source) {
        final String forumla = includeNullAndNans ? "S1=countDistinct(S1, true)" : "S1=countDistinct(S1)";
        return TableTools.newTable(TableTools.charCol(S1, source)).groupBy().update(forumla);
    }

    @Override
    public Table countDistinct(boolean includeNullAndNans, float[] source) {
        final String forumla = includeNullAndNans ? "S1=countDistinct(S1, true)" : "S1=countDistinct(S1)";
        return TableTools.newTable(TableTools.floatCol(S1, source)).groupBy().update(forumla);
    }

    @Override
    public Table countDistinct(boolean includeNullAndNans, double[] source) {
        final String forumla = includeNullAndNans ? "S1=countDistinct(S1, true)" : "S1=countDistinct(S1)";
        return TableTools.newTable(TableTools.doubleCol(S1, source)).groupBy().update(forumla);
    }
}
