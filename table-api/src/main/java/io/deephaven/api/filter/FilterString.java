package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class FilterString extends FilterBase {
    /**
     * The string operation
     */
    public enum Operation {
        /**
         * @see String#startsWith(String)
         */
        STARTS_WITH,

        /**
         * @see String#endsWith(String)
         */
        ENDS_WITH,

        /**
         * @see String#contains(CharSequence)
         */
        CONTAINS,

        /**
         * @see String#equals(Object)
         * @see String#equalsIgnoreCase(String)
         */
        EQUALS,

        /**
         * @see String#equals(Object)
         * @see String#equalsIgnoreCase(String)
         */
        NOT_EQUALS,

        /**
         * @see String#compareTo(String)
         * @see String#compareToIgnoreCase(String)
         */
        GREATER_THAN,

        /**
         * @see String#compareTo(String)
         * @see String#compareToIgnoreCase(String)
         */
        GREATER_THAN_OR_EQUAL,

        /**
         * @see String#compareTo(String)
         * @see String#compareToIgnoreCase(String)
         */
        LESS_THAN,

        /**
         * @see String#compareTo(String)
         * @see String#compareToIgnoreCase(String)
         */
        LESS_THAN_OR_EQUAL;

        /**
         * Helper to create a filter string.
         *
         * <p>
         * Equivalent to {@code of(ColumnName.of(columnName), value)}.
         *
         * @param columnName the column name
         * @param value the value
         * @return the filter string
         */
        public FilterString of(String columnName, String value) {
            return of(ColumnName.of(columnName), value);
        }

        /**
         * Helper to create a filter string.
         *
         * <p>
         * Equivalent to {@code builder().expression(expression).operation(this).value(value).build()}.
         *
         * @param expression the expression
         * @param value the value
         * @return the filter string
         */
        public FilterString of(Expression expression, String value) {
            return builder().expression(expression).operation(this).value(value).build();
        }
    }

    public static Builder builder() {
        return ImmutableFilterString.builder();
    }

    /**
     * Creates a starts-with filter pattern.
     *
     * <p>
     * Equivalent to {@code Operation.STARTS_WITH.of(columnName, value)}.
     *
     * @param columnName the column name
     * @param value the literal value
     * @return the starts-with filter pattern
     */
    public static FilterString startsWith(String columnName, String value) {
        return Operation.STARTS_WITH.of(columnName, value);
    }

    /**
     * Creates an ends-with filter pattern.
     *
     * <p>
     * Equivalent to {@code Operation.ENDS_WITH.of(columnName, value)}.
     *
     * @param columnName the column name
     * @param value the literal value
     * @return the ends-with filter pattern
     */
    public static FilterString endsWith(String columnName, String value) {
        return Operation.ENDS_WITH.of(columnName, value);
    }

    /**
     * Creates a contains filter pattern.
     *
     * <p>
     * Equivalent to {@code Operation.CONTAINS.of(columnName, value)}.
     *
     * @param columnName the column name
     * @param value the literal value
     * @return the contains filter pattern
     */
    public static FilterString contains(String columnName, String value) {
        return Operation.CONTAINS.of(columnName, value);
    }

    /**
     * Creates an equals filter pattern.
     *
     * <p>
     * Equivalent to {@code Operation.EQUALS.of(columnName, value)}.
     *
     * @param columnName the column name
     * @param value the literal value
     * @return the equals filter pattern
     */
    public static FilterString eq(String columnName, String value) {
        return Operation.EQUALS.of(columnName, value);
    }

    /**
     * Creates a not-equals filter pattern.
     *
     * <p>
     * Equivalent to {@code Operation.NOT_EQUALS.of(columnName, value)}.
     *
     * @param columnName the column name
     * @param value the literal value
     * @return the not-equals filter pattern
     */
    public static FilterString neq(String columnName, String value) {
        return Operation.NOT_EQUALS.of(columnName, value);
    }

    /**
     * Creates a greater-than filter pattern.
     *
     * <p>
     * Equivalent to {@code Operation.GREATER_THAN.of(columnName, value)}.
     *
     * @param columnName the column name
     * @param value the literal value
     * @return the greater-than filter pattern
     */
    public static FilterString gt(String columnName, String value) {
        return Operation.GREATER_THAN.of(columnName, value);
    }

    /**
     * Creates a greater-than-or-equals filter pattern.
     *
     * <p>
     * Equivalent to {@code Operation.GREATER_THAN_OR_EQUALS.of(columnName, value)}.
     *
     * @param columnName the column name
     * @param value the literal value
     * @return the greater-than-or-equals filter pattern
     */
    public static FilterString gte(String columnName, String value) {
        return Operation.GREATER_THAN_OR_EQUAL.of(columnName, value);
    }

    /**
     * Creates a less-than filter pattern.
     *
     * <p>
     * Equivalent to {@code Operation.LESS_THAN.of(columnName, value)}.
     *
     * @param columnName the column name
     * @param value the literal value
     * @return the less-than filter pattern
     */
    public static FilterString lt(String columnName, String value) {
        return Operation.LESS_THAN.of(columnName, value);
    }

    /**
     * Creates a less-than-or-equals filter pattern.
     *
     * <p>
     * Equivalent to {@code Operation.LESS_THAN_OR_EQUALS.of(columnName, value)}.
     *
     * @param columnName the column name
     * @param value the literal value
     * @return the less-than-or-equals filter pattern
     */
    public static FilterString lte(String columnName, String value) {
        return Operation.LESS_THAN_OR_EQUAL.of(columnName, value);
    }

    public abstract Expression expression();

    public abstract Operation operation();

    public abstract String value();

    @Default
    public boolean caseInsensitive() {
        return false;
    }

    /**
     * Creates a string filter with all the same values as {@code this}, except with {@link #caseInsensitive()} set to
     * {@code true}.
     *
     * @return the case-insensitive string filter
     */
    public final FilterString withCaseInsensitive() {
        if (caseInsensitive()) {
            return this;
        }
        return builder()
                .expression(expression())
                .operation(operation())
                .value(value())
                .caseInsensitive(true)
                .build();
    }

    @Override
    public final FilterNot<FilterString> invert() {
        return Filter.not(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {
        Builder expression(Expression expression);

        Builder operation(Operation operation);

        Builder value(String value);

        Builder caseInsensitive(boolean caseInsensitive);

        FilterString build();
    }
}
