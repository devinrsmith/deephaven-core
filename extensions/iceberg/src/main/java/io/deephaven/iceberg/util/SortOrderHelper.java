package io.deephaven.iceberg.util;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderBuilder;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public final class SortOrderHelper {
    private SortOrderHelper() {}

    /**
     * Checks whether this order satisfies another order. A fix for {@link SortOrder#satisfies(SortOrder)}.
     *
     * @param anotherSortOrder a different sort order
     * @return true if this order satisfies the given order
     * @see <a href="github.com/apache/iceberg/pull/18929">API: Fix SortOrder#satisfies</a>
     */
    public static boolean satisfies(SortOrder sortOrder, SortOrder anotherSortOrder) {
        // any ordering satisfies an unsorted ordering
        if (anotherSortOrder.isUnsorted()) {
            return true;
        }

        final List<SortField> anotherFields = anotherSortOrder.fields();
        final List<SortField> fields = sortOrder.fields();
        // this ordering cannot satisfy an ordering with more sort fields
        if (anotherFields.size() > fields.size()) {
            return false;
        }

        // this ordering has either more or the same number of sort fields
        int last = anotherFields.size() - 1;
        return IntStream.range(0, anotherFields.size())
                .allMatch(index -> index < last
                        ? fields.get(index).equals(anotherFields.get(index))
                        : fields.get(index).satisfies(anotherFields.get(index)));
    }

    /**
     * Constructs a {@link SortOrder} from Deephaven's {@code sortColumns} semantics; once a sort column is reached that
     * can't be expressed by Iceberg (for example, Deephaven's floating-point sort can't be expressed), the
     * partially constructed {@link SortOrder} will be returned. Callers that need to ensure all fields are mapped
     * should check the cardinality of returned {@link SortOrder}.
     *
     * @param tableDefinition the table definition
     * @param sortColumns the sort columns
     * @param deephavenColumnNameToFieldId the mapping to field ids
     * @param schema the schema
     * @return the sort order
     */
    public static SortOrder sortOrder(
            final TableDefinition tableDefinition,
            final List<SortColumn> sortColumns,
            final Map<String, Integer> deephavenColumnNameToFieldId,
            final Schema schema) {
        return createSortOrder(tableDefinition, sortColumns, schema, deephavenColumnNameToFieldId,
                () -> SortOrder.builderFor(schema)).build();
    }

    /**
     * Constructs a {@link ReplaceSortOrder} from Deephaven's {@code sortColumns} semantics; once a sort column is reached that
     * can't be expressed by Iceberg (for example, Deephaven's floating-point sort can't be expressed), the
     * partially constructed {@link ReplaceSortOrder} will be returned. Callers that need to ensure all fields are mapped
     * should check the cardinality of returned {@link ReplaceSortOrder} (via {@link ReplaceSortOrder#apply()}).
     *
     * @param tableDefinition the table definition
     * @param sortColumns the sort columns
     * @param deephavenColumnNameToFieldId the mapping to field ids
     * @param table the table
     * @return the replace sort order
     */
    public static ReplaceSortOrder replaceSortOrder(
            final TableDefinition tableDefinition,
            final List<SortColumn> sortColumns,
            final Map<String, Integer> deephavenColumnNameToFieldId,
            final org.apache.iceberg.Table table) {
        return createSortOrder(tableDefinition, sortColumns, table.schema(), deephavenColumnNameToFieldId,
                table::replaceSortOrder);
    }

    private static <Builder extends SortOrderBuilder<Builder>> Builder createSortOrder(
            final TableDefinition tableDefinition,
            final List<SortColumn> sortColumns,
            final Schema schema,
            final Map<String, Integer> deephavenColumnToFieldId,
            final Supplier<Builder> builderSupplier) {
        Builder builder = builderSupplier.get();
        for (final SortColumn sortColumn : sortColumns) {
            final SortFieldReference sortFieldRef = SortFieldReference
                    .mapToSortField(tableDefinition, sortColumn, deephavenColumnToFieldId, schema)
                    .orElse(null);
            if (sortFieldRef == null) {
                break;
            }
            builder = sortFieldRef.addTo(builder);
        }
        return builder;
    }
}
