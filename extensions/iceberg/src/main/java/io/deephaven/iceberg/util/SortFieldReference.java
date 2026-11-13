package io.deephaven.iceberg.util;

import io.deephaven.api.SortColumn;
import io.deephaven.api.SortSpec;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderBuilder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A named-based variant of {@link SortField}; allows the declarative building of {@link SortOrderBuilder}.
 */
public record SortFieldReference(
        Transform<?, ?> transform,
        String fieldName,
        SortDirection sortDirection,
        NullOrder nullOrder) {

    public SortFieldReference {
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(transform);
        Objects.requireNonNull(sortDirection);
        Objects.requireNonNull(nullOrder);
    }

    public boolean satisfies(final SortFieldReference other) {
        return equals(other)
                || (
                sortDirection == other.sortDirection
                        && nullOrder == other.nullOrder
                        && fieldName.equals(other.fieldName)
                        && transform.satisfiesOrderOf(other.transform));
    }

    /**
     * Adapts a Deephaven {@link SortSpec} into the most specific {@link SortFieldReference} when possible, such that
     * the Deephaven column, sorted on {@code sortSpec}, satisfies the returned sort field reference.
     *
     * @param tableDefinition the table definition
     * @param sortSpec the sort spec
     * @param deephavenColumnNameToFieldId the Deephaven column name to field id map
     * @param schema the schema
     * @return the sort field reference, when possible
     */
    static Optional<SortFieldReference> mapToSortField(
            final TableDefinition tableDefinition,
            final SortSpec sortSpec,
            final Map<String, Integer> deephavenColumnNameToFieldId,
            final Schema schema) {
        final Types.NestedField field;
        final String columnName = sortSpec.column().name();
        {
            final Integer fieldId = deephavenColumnNameToFieldId.get(columnName);
            if (fieldId == null) {
                throw new IllegalArgumentException("No field id found for '%s'".formatted(columnName));
            }
            field = schema.findField(fieldId);
            if (field == null) {
                throw new IllegalArgumentException("No field found for field id %d, column name '%s'".formatted(fieldId, columnName));
            }
        }
        final ColumnDefinition<?> columnDefinition = tableDefinition.getColumn(columnName);
        if (columnDefinition == null) {
            throw new IllegalArgumentException("No ColumnDefinition found for '%s'".formatted(columnName));
        }
        return SortSpecUnbound.mapToSortSpec(sortSpec, columnDefinition, field.type())
                .map(sortSpecUnbound -> yep(field.name(), sortSpecUnbound));
    }

    private static SortFieldReference yep(String fieldName, SortSpecUnbound sortSpecUnbound) {
        return new SortFieldReference(sortSpecUnbound.transform, fieldName, sortSpecUnbound.sortDirection, sortSpecUnbound.nullOrder);
    }

    public SortField toSortField(Schema schema) {
        return addTo(SortOrder.builderFor(schema)).build().fields().get(0);
    }

    public <R> R addTo(SortOrderBuilder<R> builder) {
        final UnboundTerm<?> term = Expressions.transform(fieldName, transform);
        return switch (sortDirection) {
            case ASC -> builder.asc(term, nullOrder);
            case DESC -> builder.desc(term, nullOrder);
        };
    }

    private record SortSpecUnbound(Transform<?, ?> transform, SortDirection sortDirection, NullOrder nullOrder) {
        private SortSpecUnbound {
            Objects.requireNonNull(transform);
            Objects.requireNonNull(sortDirection);
            Objects.requireNonNull(nullOrder);
        }

        private static Optional<SortSpecUnbound> mapToSortSpec(
                final SortSpec sortSpec,
                final ColumnDefinition<?> columnDefinition,
                final Type icebergType) {
            return sortSpec instanceof final SortColumn column
                    ? mapToSortSpec(column, columnDefinition, icebergType)
                    : Optional.empty();
        }

        static Optional<SortSpecUnbound> mapToSortSpec(
                final SortColumn sortColumn,
                final ColumnDefinition<?> columnDefinition,
                final Type icebergType) {
            if (!sortColumn.column().name().equals(columnDefinition.getName())) {
                throw new IllegalArgumentException("SortColumn name '%s' does not match ColumnDefinition name '%s'".formatted(sortColumn.column().name(), columnDefinition.getName()));
            }
            if (!equivalentSortOrder(columnDefinition, icebergType)) {
                return Optional.empty();
            }
            return Optional.of(new SortSpecUnbound(
                    // SortColumn is always based on the exact column / identity
                    Transforms.identity(),
                    switch (sortColumn.order()) {
                        case ASCENDING -> SortDirection.ASC;
                        case DESCENDING -> SortDirection.DESC;
                    },
                    // SortColumn is always ASCENDING with nulls first, or DESCENDING with nulls last
                    switch (sortColumn.order()) {
                        case ASCENDING -> NullOrder.NULLS_FIRST;
                        case DESCENDING -> NullOrder.NULLS_LAST;
                    }));
        }
    }

    private static boolean equivalentSortOrder(@NotNull final ColumnDefinition<?> cd, @NotNull final Type type) {
        // The switch is exhaustive with no default, so a type ID that an Iceberg upgrade adds fails compilation here,
        // forcing a decision on whether Deephaven orders the new type as the specification does
        final Class<?> dataType = cd.getDataType();
        return switch (type.typeId()) {
            case BOOLEAN -> dataType == Boolean.class;
            case INTEGER, LONG -> dataType == byte.class || dataType == short.class || dataType == int.class
                    || dataType == long.class;
            case DATE -> dataType == LocalDate.class;
            case TIME -> dataType == LocalTime.class;
            case TIMESTAMP, TIMESTAMP_NANO -> dataType == Instant.class || dataType == LocalDateTime.class;
            // Deephaven orders these differently than the specification does, as the Javadoc describes
            case FLOAT, DOUBLE, STRING -> false;
            // Deephaven does not read these types, or the two orderings have not been compared
            case DECIMAL, UUID, FIXED, BINARY, GEOMETRY, GEOGRAPHY, VARIANT, UNKNOWN, STRUCT, LIST, MAP -> false;
        };
    }
}
