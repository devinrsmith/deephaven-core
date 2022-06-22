/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import org.junit.Test;

import static io.deephaven.web.shared.ast.FilterTestUtils.*;

public class MergeNestedBinaryOperationsTest extends AbstractReplacingVisitorTest {

    @Test
    public void testMergeNestedBinaryOperations() {
        assertUnchanged("don't merge AND and OR",
                and(
                        or(
                                eq("ColumnB", 3),
                                eq("ColumnA", 4)),
                        eq("ColumnA", 1)));
        assertUnchanged("don't merge AND and OR",
                or(
                        and(
                                eq("ColumnB", 3),
                                eq("ColumnA", 4)),
                        eq("ColumnA", 1)));

        assertFilterEquals("merge ANDs",
                and(
                        eq("ColumnA", 3),
                        and(
                                eq("ColumnB", 3),
                                eq("ColumnC", 3))),
                and(
                        eq("ColumnA", 3),
                        eq("ColumnB", 3),
                        eq("ColumnC", 3)));
        assertFilterEquals("merge ANDs",
                and(
                        and(
                                eq("ColumnA", 3),
                                eq("ColumnB", 3)),
                        eq("ColumnC", 3)),
                and(
                        eq("ColumnA", 3),
                        eq("ColumnB", 3),
                        eq("ColumnC", 3)));
        assertFilterEquals("merge ORs",
                or(
                        eq("ColumnA", 3),
                        or(
                                eq("ColumnB", 3),
                                eq("ColumnC", 3))),
                or(
                        eq("ColumnA", 3),
                        eq("ColumnB", 3),
                        eq("ColumnC", 3)));
        assertFilterEquals("merge ANDs",
                or(
                        or(
                                eq("ColumnA", 3),
                                eq("ColumnB", 3)),
                        eq("ColumnC", 3)),
                or(
                        eq("ColumnA", 3),
                        eq("ColumnB", 3),
                        eq("ColumnC", 3)));
    }

    @Override
    protected FilterDescriptor execute(FilterDescriptor f) {
        return MergeNestedBinaryOperations.execute(f);
    }
}
