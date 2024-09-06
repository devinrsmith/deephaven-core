//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.FilterPattern.Mode;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.regex.Pattern;

@Immutable
@BuildableStyle
public abstract class FilterContains extends FilterBase {
    public static Builder builder() {
        return ImmutableFilterContains.builder();
    }

    public static FilterContains of(String columnName, String sequence) {
        return builder()
                .expression(ColumnName.of(columnName))
                .sequence(sequence)
                .build();
    }

    /**
     * The expression.
     */
    public abstract Expression expression();

    /**
     * The sequence.
     */
    public abstract String sequence();

    @Default
    public boolean caseSensitive() {
        return true;
    }

    @Default
    public boolean invertPattern() {
        return false;
    }

    @Override
    public final FilterNot<FilterContains> invert() {
        return Filter.not(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public final FilterPattern toFilterPattern() {
        return FilterPattern.of(
                expression(),
                Pattern.compile(Pattern.quote(sequence()), caseSensitive() ? 0 : Pattern.CASE_INSENSITIVE),
                Mode.FIND,
                invertPattern());
    }

    public interface Builder {

        Builder expression(Expression expression);

        Builder sequence(String sequence);

        Builder caseSensitive(boolean caseSensitive);

        Builder invertPattern(boolean invertPattern);

        FilterContains build();
    }
}
