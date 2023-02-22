/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIsNotNull;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.value.Literal;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
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
                : filter.walk(new Adapter(false)).getOut();
    }

    static WhereFilter ofInverted(Filter filter) {
        return filter.walk(new Adapter(true)).getOut();
    }

    static WhereFilter[] from(Collection<? extends Filter> filters) {
        return filters.stream().map(WhereFilter::of).toArray(WhereFilter[]::new);
    }

    static WhereFilter[] fromInverted(Collection<? extends Filter> filters) {
        return filters.stream().map(WhereFilter::ofInverted).toArray(WhereFilter[]::new);
    }

    static WhereFilter[] copyFrom(WhereFilter[] filters) {
        return Arrays.stream(filters).map(WhereFilter::copy).toArray(WhereFilter[]::new);
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
     * @apiNote Any {@link io.deephaven.engine.context.QueryLibrary}, {@link io.deephaven.engine.context.QueryScope}, or
     *          {@link QueryCompiler} usage needs to be resolved within init. Implementations must be idempotent.
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
     * Set the RecomputeListener that should be notified if results based on this filter must be recomputed.
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

    class Adapter implements Filter.Visitor {
        private final boolean inverted;
        private WhereFilter out;

        private Adapter(boolean inverted) {
            this.inverted = inverted;
        }

        public WhereFilter getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(FilterComparison comparison) {
            out = FilterComparisonAdapter.of(inverted ? comparison.invert() : comparison);
        }

        @Override
        public void visit(FilterNot not) {
            out = not.filter().walk(new Adapter(!inverted)).getOut();
        }

        @Override
        public void visit(FilterIsNull isNull) {
            if (inverted) {
                out = isNotNull(isNull.expression());
            } else {
                out = isNull(isNull.expression());
            }
        }

        @Override
        public void visit(FilterIsNotNull isNotNull) {
            if (inverted) {
                out = isNull(isNotNull.expression());
            } else {
                out = isNotNull(isNotNull.expression());
            }
        }

        @Override
        public void visit(FilterOr ors) {
            if (inverted) {
                // !A && !B && ... && !Z
                out = ConjunctiveFilter.makeConjunctiveFilter(fromInverted(ors.filters()));
            } else {
                // A || B || ... || Z
                out = DisjunctiveFilter.makeDisjunctiveFilter(from(ors.filters()));
            }
        }

        @Override
        public void visit(FilterAnd ands) {
            if (inverted) {
                // !A || !B || ... || !Z
                out = DisjunctiveFilter.makeDisjunctiveFilter(fromInverted(ands.filters()));
            } else {
                // A && B && ... && Z
                out = ConjunctiveFilter.makeConjunctiveFilter(from(ands.filters()));
            }
        }

        @Override
        public void visit(RawString rawString) {
            if (inverted) {
                out = WhereFilterFactory.getExpression(String.format("!(%s)", rawString.value()));
            } else {
                out = WhereFilterFactory.getExpression(rawString.value());
            }
        }

        private static WhereFilter isNull(Expression expression) {
            // todo: use a visitor
            if (expression instanceof ColumnName) {
                return new MatchFilter(((ColumnName) expression).name(), new Object[] {null});
            }
            return WhereFilterFactory.getExpression(Strings.of(Filter.isNull(expression)));
        }

        private static WhereFilter isNotNull(Expression expression) {
            // todo: use a visitor
            if (expression instanceof ColumnName) {
                return new MatchFilter(MatchType.Inverted, ((ColumnName) expression).name(), new Object[] {null});
            }
            return WhereFilterFactory.getExpression(Strings.of(Filter.isNotNull(expression)));
        }

        private static class FilterComparisonAdapter implements Expression.Visitor, Literal.Visitor {

            public static WhereFilter of(FilterComparison condition) {
                FilterComparison preferred = condition.maybeTranspose();
                return preferred.lhs().walk(new FilterComparisonAdapter(condition, preferred)).getOut();
            }

            private final FilterComparison original;
            private final FilterComparison preferred;

            private WhereFilter out;

            private FilterComparisonAdapter(FilterComparison original, FilterComparison preferred) {
                this.original = Objects.requireNonNull(original);
                this.preferred = Objects.requireNonNull(preferred);
            }

            public WhereFilter getOut() {
                return Objects.requireNonNull(out);
            }

            @Override
            public void visit(ColumnName lhs) {
                preferred.rhs().walk(new PreferredLhsColumnRhsVisitor(lhs));
            }

            private class PreferredLhsColumnRhsVisitor implements Expression.Visitor, Literal.Visitor {
                private final ColumnName lhs;

                public PreferredLhsColumnRhsVisitor(ColumnName lhs) {
                    this.lhs = Objects.requireNonNull(lhs);
                }

                @Override
                public void visit(ColumnName rhs) {
                    // LHS column = RHS column
                    out = WhereFilterFactory.getExpression(Strings.of(original));
                }

                @Override
                public void visit(long rhs) {
                    switch (preferred.operator()) {
                        case LESS_THAN:
                            out = new LongRangeFilter(lhs.name(), Long.MIN_VALUE, rhs, true, false);
                            break;
                        case LESS_THAN_OR_EQUAL:
                            out = new LongRangeFilter(lhs.name(), Long.MIN_VALUE, rhs, true, true);
                            break;
                        case GREATER_THAN:
                            out = new LongRangeFilter(lhs.name(), rhs, Long.MAX_VALUE, false, true);
                            break;
                        case GREATER_THAN_OR_EQUAL:
                            out = new LongRangeFilter(lhs.name(), rhs, Long.MAX_VALUE, true, true);
                            break;
                        case EQUALS:
                            out = new MatchFilter(lhs.name(), rhs);
                            break;
                        case NOT_EQUALS:
                            out = new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected operator " + original.operator());
                    }
                }

                @Override
                public void visit(boolean rhs) {
                    switch (preferred.operator()) {
                        case EQUALS:
                            out = new MatchFilter(lhs.name(), rhs);
                            break;
                        case NOT_EQUALS:
                            out = new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                            break;
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            out = WhereFilterFactory.getExpression(Strings.of(original));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected operator " + original.operator());
                    }
                }

                @Override
                public void visit(Filter rhs) {
                    out = WhereFilterFactory.getExpression(Strings.of(original));
                }

                @Override
                public void visit(Literal value) {
                    value.walk((Literal.Visitor) this);
                }

                @Override
                public void visit(RawString rawString) {
                    out = WhereFilterFactory.getExpression(Strings.of(original));
                }
            }

            // Note for all remaining cases: since we are walking the preferred object, we know we don't have to handle
            // the case where rhs is column name.

            @Override
            public void visit(long lhs) {
                out = WhereFilterFactory.getExpression(Strings.of(original));
            }

            @Override
            public void visit(boolean x) {
                out = WhereFilterFactory.getExpression(Strings.of(original));
            }

            @Override
            public void visit(Literal lhs) {
                lhs.walk((Literal.Visitor) this);
            }

            @Override
            public void visit(Filter lhs) {
                out = WhereFilterFactory.getExpression(Strings.of(original));
            }

            @Override
            public void visit(RawString lhs) {
                out = WhereFilterFactory.getExpression(Strings.of(original));
            }
        }
    }

    // region Filter impl

    @Override
    default FilterNot not() {
        throw new UnsupportedOperationException("WhereFilters do not implement not");
    }

    @Override
    default <V extends Visitor> V walk(V visitor) {
        throw new UnsupportedOperationException("WhereFilters do not implement walk");
    }

    @Override
    default <V extends Expression.Visitor> V walk(V visitor) {
        throw new UnsupportedOperationException("WhereFilters do not implement walk");
    }

    // endregion Filter impl
}
