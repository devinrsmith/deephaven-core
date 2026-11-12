//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.api.SortColumn;
import org.apache.iceberg.SortOrder;

import java.util.List;
import java.util.Objects;

/**
 * A specification for providing {@link org.apache.iceberg.SortOrder} while writing to an iceberg table.
 * <p>
 * Deephaven sorts new data by the provided sort order with its own comparisons, which differ from the Iceberg
 * specification's for strings and floating point values. New data files declare the sort order in the Iceberg metadata
 * only where the two agree: every field of the sort order must be on a boolean, integer, date, time or timestamp
 * column, and use the identity transform, except that the last field may use any order preserving transform, such as
 * {@code day}. Otherwise new data files declare no sort order. Either way, Deephaven also records the sort's leading
 * column in its own metadata in each data file.
 */
public sealed interface SortOrderProvider {

    record Unsorted() implements SortOrderProvider {}

    record UseTableDefaults(boolean failOnUnmapped) implements SortOrderProvider {

        public UseTableDefaults withFailOnUnmapped(boolean failOnUnmapped) {
            return new UseTableDefaults(failOnUnmapped);
        }
    }

    record SortOrderDirect(SortOrder sortOrder, boolean failOnUnmapped) implements SortOrderProvider {
        public SortOrderDirect {
            Objects.requireNonNull(sortOrder);
        }

        public SortOrderDirect withFailOnUnmapped(boolean failOnUnmapped) {
            return new SortOrderDirect(sortOrder, failOnUnmapped);
        }
    }

    record SortOrderId(int id, boolean failOnUnmapped) implements SortOrderProvider {

        public SortOrderId withFailOnUnmapped(boolean failOnUnmapped) {
            return new SortOrderId(id, failOnUnmapped);
        }
    }

    record SortColumns(List<SortColumn> columns) implements SortOrderProvider {

        public SortColumns {
            Objects.requireNonNull(columns);
        }
    }

    /**
     * Do not sort the data while writing new data to the iceberg table.
     */
    static Unsorted unsorted() {
        return new Unsorted();
    }

    /**
     * Use the default {@link org.apache.iceberg.Table#sortOrder()} of the table while writing new data. If no sort
     * order is set on the table, no sorting will be done.
     */
    static UseTableDefaults useTableDefault() {
        return new UseTableDefaults(true);
    }

    /**
     * Use the sort order with the given ID to sort new data while writing to the iceberg table.
     */
    static SortOrderId fromSortId(final int id) {
        return new SortOrderId(id, true);
    }

    /**
     * Use the given sort order directly to sort new data while writing to the iceberg table. Note that the provided
     * sort order must either have a valid {@link SortOrder#orderId()}, else this provider should be chained with an
     * {@link #withId(int)} call to set a valid order ID.
     */
    static SortOrderDirect fromSortOrder(final SortOrder sortOrder) {
        return new SortOrderDirect(sortOrder, true);
    }

    /**
     * Returns a sort order provider that uses the current provider to determine the columns to sort on, but writes a
     * different sort order ID to the Iceberg table. Note that the sort order returned by the caller must
     * {@link SortOrder#satisfies(SortOrder) satisfy} the sort order corresponding to the provided sort order ID.
     * <p>
     * For example, this provider might sort by columns {A, B, C}, but the ID written to Iceberg corresponds to a sort
     * order with columns {A, B}, perhaps because C is a string column. The ID is written only where Deephaven can
     * declare the sort order with that ID, as the {@link SortOrderProvider} documentation describes, and is still
     * checked otherwise.
     *
     * @param sortOrderId the sort order ID to write to the iceberg table
     */
    default SortOrderProvider withId(final int sortOrderId) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a sort order provider configured to fail (or not) if the sort order cannot be applied to the tables being
     * written. By default, all providers fail if the sort order cannot be applied.
     *
     * @param failOnUnmapped whether to fail if the sort order cannot be applied to the tables being written. If
     *        {@code false} and the sort order cannot be applied, the tables will be written without sorting.
     */
    //SortOrderProvider withFailOnUnmapped(boolean failOnUnmapped);
}
