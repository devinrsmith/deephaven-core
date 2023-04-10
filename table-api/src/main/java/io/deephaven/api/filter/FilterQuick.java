package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class FilterQuick extends FilterBase {

    public static Builder builder() {
        return ImmutableFilterQuick.builder();
    }

    public static FilterQuick of(String quickFilter) {
        return builder().quickFilter(quickFilter).build();
    }

    public abstract String quickFilter();

    public abstract List<ColumnName> columns();

    @Override
    public final FilterNot<FilterQuick> invert() {
        return FilterNot.of(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {
        Builder quickFilter(String quickFilter);

        Builder addColumns(ColumnName elements);

        Builder addColumns(ColumnName... elements);

        Builder addAllColumns(Iterable<? extends ColumnName> elements);

        FilterQuick build();
    }
}
