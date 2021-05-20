package io.deephaven.qst;

import java.util.List;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class TableHeader {

    public static TableHeader empty() {
        return ImmutableTableHeader.builder().build();
    }

    public static TableHeader of(ColumnHeader<?>... headers) {
        return ImmutableTableHeader.builder().addHeaders(headers).build();
    }

    public static TableHeader of(Iterable<ColumnHeader<?>> headers) {
        return ImmutableTableHeader.builder().addAllHeaders(headers).build();
    }

    public abstract List<ColumnHeader<?>> headers();

    @Check
    final void checkDistinctColumnNames() {
        if (headers().size() != headers().stream().map(ColumnHeader::name).distinct().count()) {
            throw new IllegalArgumentException("All headers must have distinct names");
        }
    }
}
