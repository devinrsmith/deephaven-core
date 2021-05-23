package io.deephaven.qst;

import java.util.List;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Column<T> {

    public static <T> Column<T> of(String name, Class<T> clazz, Iterable<T> values) {
        return ColumnHeader.of(name, clazz).withData(values);
    }

    public static <T> Column<T> of(String name, Class<T> clazz, T... values) {
        return ColumnHeader.of(name, clazz).withData(values);
    }

    public static Column<Integer> of(String name, Integer... values) {
        return ColumnHeader.ofInt(name).withData(values);
    }

    public static Column<Double> of(String name, Double... values) {
        return ColumnHeader.ofDouble(name).withData(values);
    }

    public static Column<String> of(String name, String... values) {
        return ColumnHeader.ofString(name).withData(values);
    }

    public static <T> Column<T> empty(ColumnHeader<T> header) {
        return ImmutableColumn.<T>builder().header(header).build();
    }

    public static <T> Column<T> of(ColumnHeader<T> header, T... data) {
        return ImmutableColumn.<T>builder().header(header).addValues(data).build();
    }

    public static <T> Column<T> of(ColumnHeader<T> header, Iterable<T> data) {
        return ImmutableColumn.<T>builder().header(header).addAllValues(data).build();
    }

    public abstract ColumnHeader<T> header();

    @AllowNulls
    public abstract List<T> values();

    public final String name() {
        return header().name();
    }

    public final ColumnType<T> type() {
        return header().type();
    }

    public final int size() {
        return values().size();
    }

    public final NewTable toTable() {
        return NewTable.of(this);
    }

    @Check
    final void checkValues() {
        for (T value : values()) {
            if (!type().isValidValue(value)) {
                throw new IllegalArgumentException(String.format("Invalid value: %s", value));
            }
        }
    }
}
