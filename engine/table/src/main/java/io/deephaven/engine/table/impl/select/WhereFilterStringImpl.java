/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.filter.FilterString;
import io.deephaven.api.filter.FilterString.Operation;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter.ObjectChunkFilter;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

class WhereFilterStringImpl extends WhereFilterImpl {

    private static final long serialVersionUID = 1L;

    public static WhereFilter of(FilterString filter, boolean inverted) {
        final FilterPattern filterPattern = toFilterPatternForImpl(filter).orElse(null);
        return filterPattern != null
                ? WhereFilterAdapter.of(filterPattern, inverted)
                : new WhereFilterStringImpl(filter, inverted);
    }

    private final FilterString filter;
    private final boolean inverted;
    private transient ObjectChunkFilter<String> chunkFilterImpl;

    private WhereFilterStringImpl(FilterString filter, boolean inverted) {
        this.filter = Objects.requireNonNull(filter);
        this.inverted = inverted;
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        final String columnName = columnName();
        final ColumnDefinition<?> column = tableDefinition.getColumn(columnName);
        if (column == null) {
            throw new RuntimeException(String.format("Column '%s' doesn't exist in this table, available columns: %s",
                    columnName, tableDefinition.getColumnNames()));
        }
        final ObjectChunkFilterSimpleNotNullBase<String> modeChunkFilter = modeChunkFilter();
        chunkFilterImpl = inverted ? modeChunkFilter.invertFilter() : modeChunkFilter;
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        final ColumnSource<String> columnSource = table.getColumnSource(columnName());
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilterImpl);
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {

    }

    @Override
    public boolean canMemoize() {
        return true;
    }

    @Override
    public List<String> getColumns() {
        return Collections.singletonList(columnName());
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public WhereFilter copy() {
        return new WhereFilterStringImpl(filter, inverted);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WhereFilterStringImpl that = (WhereFilterStringImpl) o;
        if (inverted != that.inverted)
            return false;
        return filter.equals(that.filter);
    }

    @Override
    public int hashCode() {
        int result = filter.hashCode();
        result = 31 * result + (inverted ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WhereFilterStringImpl{" +
                "filter=" + filter +
                ", inverted=" + inverted +
                '}';
    }

    private ObjectChunkFilterSimpleNotNullBase<String> modeChunkFilter() {
        switch (filter.operation()) {
            case STARTS_WITH:
                if (filter.caseInsensitive()) {
                    throw new IllegalStateException();
                }
                return new StartsWith();
            case ENDS_WITH:
                if (filter.caseInsensitive()) {
                    throw new IllegalStateException();
                }
                return new EndsWith();
            case CONTAINS:
                if (filter.caseInsensitive()) {
                    throw new IllegalStateException();
                }
                return new Contains();
            case EQUALS:
                return filter.caseInsensitive() ? new EQCI() : new EQ();
            case NOT_EQUALS:
                return filter.caseInsensitive() ? new NEQCI() : new NEQ();
            case GREATER_THAN:
                return filter.caseInsensitive() ? new GTCI() : new GT();
            case GREATER_THAN_OR_EQUAL:
                return filter.caseInsensitive() ? new GTECI() : new GTE();
            case LESS_THAN:
                return filter.caseInsensitive() ? new LTCI() : new LT();
            case LESS_THAN_OR_EQUAL:
                return filter.caseInsensitive() ? new LTECI() : new LTE();
            default:
                throw new IllegalArgumentException("Unexpected filter mode " + filter.operation());
        }
    }

    private String columnName() {
        return ((ColumnName) filter.expression()).name();
    }

    class StartsWith extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.startsWith(filter.value());
        }
    }

    class EndsWith extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.endsWith(filter.value());
        }
    }

    class Contains extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.contains(filter.value());
        }
    }

    class EQ extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.equals(filter.value());
        }
    }

    class EQCI extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.equalsIgnoreCase(filter.value());
        }
    }

    class NEQ extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return !s.equals(filter.value());
        }
    }

    class NEQCI extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return !s.equalsIgnoreCase(filter.value());
        }
    }

    class GT extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.compareTo(filter.value()) > 0;
        }
    }

    class GTE extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.compareTo(filter.value()) >= 0;
        }
    }

    class LT extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.compareTo(filter.value()) < 0;
        }
    }

    class LTE extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.compareTo(filter.value()) <= 0;
        }
    }

    class GTCI extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.compareToIgnoreCase(filter.value()) > 0;
        }
    }

    class GTECI extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.compareToIgnoreCase(filter.value()) >= 0;
        }
    }

    class LTCI extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.compareToIgnoreCase(filter.value()) < 0;
        }
    }

    class LTECI extends ObjectChunkFilterSimpleNotNullBase<String> {
        @Override
        public boolean filter(@NotNull String s) {
            return s.compareToIgnoreCase(filter.value()) <= 0;
        }
    }

    /**
     * Java does not currently have optimized implementations for case-insensitive {@link Operation#STARTS_WITH},
     * {@link Operation#ENDS_WITH}, nor {@link Operation#CONTAINS} operations. This constructs the equivalent
     * {@link FilterPattern} for those cases.
     */
    private static Optional<FilterPattern> toFilterPatternForImpl(FilterString filter) {
        if (!filter.caseInsensitive()) {
            return Optional.empty();
        }
        switch (filter.operation()) {
            case STARTS_WITH:
                return Optional.of(FilterPattern.builder()
                        .expression(filter.expression())
                        .mode(FilterPattern.Mode.FIND)
                        .pattern(Pattern.compile("^" + Pattern.quote(filter.value()), Pattern.CASE_INSENSITIVE))
                        .build());
            case ENDS_WITH:
                return Optional.of(FilterPattern.builder()
                        .expression(filter.expression())
                        .mode(FilterPattern.Mode.FIND)
                        .pattern(Pattern.compile(Pattern.quote(filter.value()) + "$", Pattern.CASE_INSENSITIVE))
                        .build());
            case CONTAINS:
                return Optional.of(FilterPattern.builder()
                        .expression(filter.expression())
                        .mode(FilterPattern.Mode.FIND)
                        .pattern(Pattern.compile(filter.value(), Pattern.LITERAL | Pattern.CASE_INSENSITIVE))
                        .build());
        }
        return Optional.empty();
    }
}
