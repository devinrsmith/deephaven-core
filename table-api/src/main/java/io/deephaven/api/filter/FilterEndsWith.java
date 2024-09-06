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
public abstract class FilterEndsWith extends FilterBase {

    public static Builder builder() {
        return ImmutableFilterEndsWith.builder();
    }

    public static FilterEndsWith of(String columnName, String suffix) {
        return builder()
                .expression(ColumnName.of(columnName))
                .suffix(suffix)
                .build();
    }

    public abstract Expression expression();

    public abstract String suffix();

    @Default
    public boolean caseSensitive() {
        return true;
    }

    @Default
    public boolean invertPattern() {
        return false;
    }

    @Override
    public final FilterNot<FilterEndsWith> invert() {
        return Filter.not(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return null;
    }

    public interface Builder {

        Builder expression(Expression expression);

        Builder suffix(String suffix);

        Builder caseSensitive(boolean caseSensitive);

        Builder invertPattern(boolean invertPattern);

        FilterEndsWith build();
    }
}
