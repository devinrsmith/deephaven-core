//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class FilterStartsWith extends FilterBase {

    public static Builder builder() {
        return ImmutableFilterStartsWith.builder();
    }

    public static FilterStartsWith of(String columnName, String prefix) {
        return builder()
                .expression(ColumnName.of(columnName))
                .prefix(prefix)
                .build();
    }

    public abstract Expression expression();

    public abstract String prefix();

    @Default
    public boolean caseSensitive() {
        return true;
    }

    @Default
    public boolean invertPattern() {
        return false;
    }

    @Override
    public final FilterNot<FilterStartsWith> invert() {
        return Filter.not(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {

        Builder expression(Expression expression);

        Builder prefix(String prefix);

        Builder caseSensitive(boolean caseSensitive);

        Builder invertPattern(boolean invertPattern);

        FilterStartsWith build();
    }
}
