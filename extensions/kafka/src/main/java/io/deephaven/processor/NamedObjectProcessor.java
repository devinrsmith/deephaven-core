/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class NamedObjectProcessor<T> {

    public static <T> Builder<T> builder() {
        return ImmutableNamedObjectProcessor.builder();
    }

    public static <T> NamedObjectProcessor<T> of(ObjectProcessor<T> processor, String... names) {
        return NamedObjectProcessor.<T>builder().processor(processor).addColumnNames(names).build();
    }

    public static <T> NamedObjectProcessor<T> of(ObjectProcessor<T> processor, Iterable<String> names) {
        return NamedObjectProcessor.<T>builder().processor(processor).addAllColumnNames(names).build();
    }

    public abstract ObjectProcessor<T> processor();

    public abstract List<String> columnNames();

    public interface Builder<T> {
        Builder<T> processor(ObjectProcessor<T> processor);

        Builder<T> addColumnNames(String element);

        Builder<T> addColumnNames(String... elements);

        Builder<T> addAllColumnNames(Iterable<String> elements);

        NamedObjectProcessor<T> build();
    }

    @Check
    final void checkSizes() {
        if (columnNames().size() != processor().size()) {
            throw new IllegalArgumentException(
                    String.format("Unmatched sizes; columnNames().size()=%d, processor().size()=%d",
                            columnNames().size(), processor().size()));
        }
    }
}
