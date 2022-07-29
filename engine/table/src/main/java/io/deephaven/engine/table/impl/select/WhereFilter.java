/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import io.deephaven.api.filter.*;
import io.deephaven.api.value.Value;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Interface for individual filters within a where clause.
 */
public interface WhereFilter extends Filter {

    static WhereFilter of(Filter filter) {
        return (filter instanceof WhereFilter)
                ? (WhereFilter) filter
                : filter.walk(new Adapter(false));
    }

    static WhereFilter ofInverted(Filter filter) {
        return filter.walk(new Adapter(true));
    }

    static WhereFilter[] from(Collection<? extends Filter> filters) {
        return filters.stream().map(WhereFilter::of).toArray(WhereFilter[]::new);
    }

    static WhereFilter[] fromInverted(Collection<? extends Filter> filters) {
        return filters.stream().map(WhereFilter::ofInverted).toArray(WhereFilter[]::new);
    }

    /**
     * Users of WhereFilter may implement this interface if they must react to the filter fundamentally changing.
     *
     * @see DynamicWhereFilter
     */
    interface RecomputeListener {
        /**
         * Notify the listener that its result must be recomputed.
         */
        void requestRecompute();

        /**
         * Notify the something about the filters has changed such that all unmatched rows of the source table should be
         * re-evaluated.
         */
        void requestRecomputeUnmatched();

        /**
         * Notify the something about the filters has changed such that all matched rows of the source table should be
         * re-evaluated.
         */
        void requestRecomputeMatched();

        /**
         * Get the table underlying this listener.
         *
         * @return the underlying table
         */
        @NotNull
        QueryTable getTable();

        /**
         * Set the filter and the table refreshing or not.
         */
        void setIsRefreshing(boolean refreshing);
    }

    WhereFilter[] ZERO_LENGTH_SELECT_FILTER_ARRAY = new WhereFilter[0];

    /**
     * Get the columns required by this select filter.
     *
     * @return the columns used as input by this select filter.
     */
    List<String> getColumns();

    /**
     * Get the array columns required by this select filter.
     *
     * @return the columns used as array input by this select filter.
     */
    List<String> getColumnArrays();

    /**
     * Initialize this select filter given the table definition
     * 
     * @param tableDefinition the definition of the table that will be filtered
     */
    void init(TableDefinition tableDefinition);

    /**
     * Filter selection to only matching rows.
     *
     * @param selection the indices that should be filtered. The selection must be a subset of fullSet, and may include
     *        rows that the engine determines need not be evaluated to produce the result. Implementations <em>may
     *        not</em> mutate or {@link RowSet#close() close} {@code selection}.
     * @param fullSet the complete RowSet of the table to filter. The fullSet is used for calculating variables like "i"
     *        or "ii". Implementations <em>may not</em> mutate or {@link RowSet#close() close} {@code fullSet}.
     * @param table the table to filter
     * @param usePrev true if previous values should be used. Implementing previous value filtering is optional, and a
     *        {@link PreviousFilteringNotSupported} exception may be thrown. If a PreviousFiltering exception is thrown,
     *        then the caller must acquire the UpdateGraphProcessor lock.
     *
     * @return The subset of selection accepted by this filter; ownership passes to the caller
     */
    WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev);

    /**
     * @return true if this is a filter that does not require any code execution, but rather is handled entirely within
     *         the database engine.
     */
    boolean isSimpleFilter();

    /**
     * Is this filter refreshing?
     *
     * @return if this filter is refreshing
     */
    default boolean isRefreshing() {
        return false;
    }

    /**
     * Set the ShiftObliviousListener that should be notified if results based on this filter must be recomputed.
     *
     * @param result the listener to notify.
     */
    void setRecomputeListener(RecomputeListener result);

    /**
     * The database system may automatically generate a filter, for example, when applying an ACL to a table. There are
     * certain operations which may bypass these filters.
     *
     * This function returns whether or not this filter is automated.
     *
     * @return true if this filter was automatically applied by the database system. False otherwise.
     */
    boolean isAutomatedFilter();

    /**
     * The database system may automatically generate a filter, for example, when applying an ACL to a table. There are
     * certain operations which may bypass these filters.
     *
     * This function indicates that this filter is automated.
     *
     * @param value true if this filter was automatically applied by the database system. False otherwise.
     */
    void setAutomatedFilter(boolean value);

    /**
     * Can this filter operation be memoized?
     *
     * @return if this filter can be memoized
     */
    default boolean canMemoize() {
        return false;
    }

    /**
     * Create a copy of this WhereFilter.
     *
     * @return an independent copy of this WhereFilter.
     */
    WhereFilter copy();

    /**
     * This exception is thrown when a where() filter is incapable of handling previous values, and thus needs to be
     * executed while under the UGP lock.
     */
    class PreviousFilteringNotSupported extends ConstructSnapshot.NoSnapshotAllowedException {
        public PreviousFilteringNotSupported() {
            super();
        }

        public PreviousFilteringNotSupported(String message) {
            super(message);
        }
    }

    class Adapter implements Filter.Visitor<WhereFilter> {
        private final boolean inverted;

        private Adapter(boolean inverted) {
            this.inverted = inverted;
        }

        @Override
        public WhereFilter visit(FilterCondition condition) {
            return FilterConditionAdapter.of(inverted ? condition.invert() : condition);
        }

        @Override
        public WhereFilter visit(FilterNot not) {
            return not.filter().walk(new Adapter(!inverted));
        }

        @Override
        public WhereFilter visit(FilterIsNull isNull) {
            if (inverted) {
                return isNotNull(isNull.column());
            } else {
                return isNull(isNull.column());
            }
        }

        @Override
        public WhereFilter visit(FilterIsNotNull isNotNull) {
            if (inverted) {
                return isNull(isNotNull.column());
            } else {
                return isNotNull(isNotNull.column());
            }
        }

        @Override
        public WhereFilter visit(FilterOr ors) {
            if (inverted) {
                // !A && !B && ... && !Z
                return ConjunctiveFilter.makeConjunctiveFilter(fromInverted(ors.filters()));
            } else {
                // A || B || ... || Z
                return DisjunctiveFilter.makeDisjunctiveFilter(from(ors.filters()));
            }
        }

        @Override
        public WhereFilter visit(FilterAnd ands) {
            if (inverted) {
                // !A || !B || ... || !Z
                return DisjunctiveFilter.makeDisjunctiveFilter(fromInverted(ands.filters()));
            } else {
                // A && B && ... && Z
                return ConjunctiveFilter.makeConjunctiveFilter(from(ands.filters()));
            }
        }

        @Override
        public WhereFilter visit(RawString rawString) {
            if (inverted) {
                return WhereFilterFactory.getExpression(String.format("!(%s)", rawString.value()));
            } else {
                return WhereFilterFactory.getExpression(rawString.value());
            }
        }

        private static MatchFilter isNull(ColumnName columnName) {
            return new MatchFilter(columnName.name(), new Object[] {null});
        }

        private static MatchFilter isNotNull(ColumnName columnName) {
            return new MatchFilter(MatchType.Inverted, columnName.name(), new Object[] {null});
        }

        private static class FilterConditionAdapter implements Value.Visitor<WhereFilter> {

            public static WhereFilter of(FilterCondition condition) {
                FilterCondition preferred = condition.maybeTranspose();
                return preferred.lhs().walk(new FilterConditionAdapter(condition, preferred));
            }

            private final FilterCondition original;
            private final FilterCondition preferred;

            private FilterConditionAdapter(FilterCondition original, FilterCondition preferred) {
                this.original = Objects.requireNonNull(original);
                this.preferred = Objects.requireNonNull(preferred);
            }

            @Override
            public WhereFilter visit(ColumnName lhs) {
                return preferred.rhs().walk(new Value.Visitor<WhereFilter>() {
                    @Override
                    public WhereFilter visit(ColumnName rhs) {
                        return WhereFilterFactory.getExpression(Strings.of(original));
                    }

                    @Override
                    public WhereFilter visit(long rhs) {
                        switch (preferred.operator()) {
                            case LESS_THAN:
                                return new LongRangeFilter(lhs.name(), Long.MIN_VALUE, rhs, true, false);
                            case LESS_THAN_OR_EQUAL:
                                return new LongRangeFilter(lhs.name(), Long.MIN_VALUE, rhs, true, true);
                            case GREATER_THAN:
                                return new LongRangeFilter(lhs.name(), rhs, Long.MAX_VALUE, false, true);
                            case GREATER_THAN_OR_EQUAL:
                                return new LongRangeFilter(lhs.name(), rhs, Long.MAX_VALUE, true, true);
                            case EQUALS:
                                return new MatchFilter(lhs.name(), rhs);
                            case NOT_EQUALS:
                                return new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                            default:
                                throw new IllegalStateException("Unexpected operator " + original.operator());
                        }
                    }
                });
            }

            // Note for all remaining cases: since we are walking the preferred object, we know we don't have to handle
            // the case where rhs is column name.

            @Override
            public WhereFilter visit(long lhs) {
                return WhereFilterFactory.getExpression(Strings.of(original));
            }
        }
    }

    // region Filter impl

    @Override
    default FilterNot not() {
        throw new UnsupportedOperationException("WhereFilters do not implement not");
    }

    @Override
    default <T> T walk(Visitor<T> visitor) {
        throw new UnsupportedOperationException("WhereFilters do not implement walk");
    }

    // endregion Filter impl
}
