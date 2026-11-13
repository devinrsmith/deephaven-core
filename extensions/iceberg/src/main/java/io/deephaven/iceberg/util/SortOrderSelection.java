//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;

/**
 * Utilities for choosing which of a table's registered {@link SortOrder}s to declare on data files that were written in
 * some desired order.
 *
 * <p>
 * A data file may only reference a sort order registered in the table metadata (or the unsorted order, ID {@code 0}).
 * When data is written in an order that is not registered, the best declaration is the most specific registered order
 * that the written data still satisfies.
 *
 * <p>
 * "Satisfies" follows {@link SortOrderHelper#satisfies(SortOrder, SortOrder)}: {@code satisfies(a, b)} means data sorted by {@code a} is
 * also correctly sorted by {@code b}. This is a partial order, so two candidates can be incomparable (for example
 * {@code [ts]} and {@code [day(ts), symbol]} when the desired order is {@code [ts, symbol]}). Incomparable candidates
 * are resolved with a caller-supplied or default tie-breaker.
 *
 * <p>
 * All orders are compared by source field ID, so the desired order must be built against a schema whose field IDs match
 * the table's (normally {@code table.schema()}).
 */
final class SortOrderSelection {

    private SortOrderSelection() {}

    /**
     * Returns the most specific sort order registered on {@code table} that {@code desired} satisfies, using
     * {@link #defaultTieBreaker(int)} for incomparable candidates.
     *
     * @param table the table whose registered sort orders are considered
     * @param desired the order the data was actually written in
     * @return the best order to declare; {@link SortOrder#unsorted()} if no registered sorted order is satisfied
     * @see #bestDeclaredOrder(Collection, SortOrder, Comparator)
     */
    public static SortOrder bestDeclaredOrder(Table table, SortOrder desired) {
        Objects.requireNonNull(table, "table");
        return bestDeclaredOrder(
                table.sortOrders().values(), desired, defaultTieBreaker(table.sortOrder().orderId()));
    }

    /**
     * Returns the most specific candidate that {@code desired} satisfies.
     *
     * <p>
     * Selection proceeds in three steps:
     *
     * <ol>
     * <li>Keep only candidates that {@code desired} satisfies. {@link SortOrder#unsorted()} is always included as a
     * fallback, since every order satisfies it.
     * <li>Discard any candidate that another remaining candidate is
     * {@linkplain #strictlyMoreSpecific(SortOrder, SortOrder) strictly more specific} than. For example, {@code [ts]}
     * eliminates {@code [day(ts)]}.
     * <li>Among the survivors, which are mutually equivalent or incomparable, return the greatest according to
     * {@code tieBreaker}.
     * </ol>
     *
     * @param candidates the registered sort orders to choose from
     * @param desired the order the data was actually written in
     * @param tieBreaker orders survivors so that the preferred one compares greatest
     * @return the best order to declare; never {@code null}
     */
    public static SortOrder bestDeclaredOrder(
            Collection<SortOrder> candidates, SortOrder desired, Comparator<SortOrder> tieBreaker) {
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(desired, "desired");
        Objects.requireNonNull(tieBreaker, "tieBreaker");

        List<SortOrder> satisfied = new ArrayList<>();
        satisfied.add(SortOrder.unsorted());
        for (SortOrder candidate : candidates) {
            if (!candidate.isUnsorted() && SortOrderHelper.satisfies(desired, candidate)) {
                satisfied.add(candidate);
            }
        }

        List<SortOrder> maximal =
                satisfied.stream()
                        .filter(c -> satisfied.stream().noneMatch(other -> strictlyMoreSpecific(other, c)))
                        .toList();

        return maximal.stream().max(tieBreaker).orElseGet(SortOrder::unsorted);
    }

    /**
     * Returns whether {@code a} is strictly more specific than {@code b}: data sorted by {@code a} is also sorted by
     * {@code b}, but not the other way around.
     *
     * @param a the first order
     * @param b the second order
     * @return {@code true} if {@code a} satisfies {@code b} and {@code b} does not satisfy {@code a}
     */
    public static boolean strictlyMoreSpecific(SortOrder a, SortOrder b) {
        return SortOrderHelper.satisfies(a, b) && !SortOrderHelper.satisfies(b, a);
    }

    /**
     * Returns the default tie-breaker for incomparable candidates. It prefers, in order:
     *
     * <ol>
     * <li>more sort fields;
     * <li>the table's default sort order;
     * <li>the higher (more recently added) order ID, for determinism.
     * </ol>
     *
     * <p>
     * Preferring more fields is a heuristic. If readers mostly prune on the leading column, a comparator that favors
     * the finest leading field may be a better fit.
     *
     * @param defaultOrderId the ID of the table's current default sort order
     * @return a comparator under which the preferred order compares greatest
     */
    public static Comparator<SortOrder> defaultTieBreaker(int defaultOrderId) {
        return Comparator.comparingInt((SortOrder o) -> o.fields().size())
                .thenComparing(o -> o.orderId() == defaultOrderId)
                .thenComparingInt(SortOrder::orderId);
    }
}
