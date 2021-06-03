package io.deephaven.qst;

import io.deephaven.qst.ColumnType.Visitor;
import java.util.List;
import java.util.Objects;
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

    public static <T> ColumnBuilder<T> builder(ColumnHeader<T> header) {
        return ImmutableColumn.<T>builder().header(header);
    }

    public static <T> Column<T> cast(@SuppressWarnings("unused") ColumnType<T> type, Column<?> column) {
        //noinspection unchecked
        return (Column<T>)column;
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

    public final <R> Column<R> into(TypeLogic logic, ColumnType<R> intoType) {
        return Column.of(
            ColumnHeader.of(name(), intoType),
            logic.transform(intoType, type(), values()));
    }

    @Check
    final void checkValues() {
        for (T value : values()) {
            if (value == null) {
                continue;
            }
            type().walk(new CheckValidValue(value));
        }
    }

    abstract static class Builder<T> implements ColumnBuilder<T> {
        @Override
        public final ColumnBuilder<T> add(T item) {
            return addValues(item);
        }

        abstract Builder<T> addValues(T element);
    }

    private static class CheckValidValue implements Visitor {

        private final Object in;

        CheckValidValue(Object in) {
            this.in = Objects.requireNonNull(in);
        }

        @Override
        public void visit(BooleanType booleanType) {

        }

        @Override
        public void visit(ByteType byteType) {
            if (byteType.castValue(in) == Byte.MIN_VALUE) {
                throw new IllegalArgumentException("Unable to represent Byte.MIN_VALUE with ByteType column");
            }
        }

        @Override
        public void visit(CharType charType) {
            if (charType.castValue(in) == Character.MAX_VALUE - 1) {
                throw new IllegalArgumentException("Unable to represent Character.MAX_VALUE - 1 with CharType column");
            }
        }

        @Override
        public void visit(ShortType shortType) {
            if (shortType.castValue(in) == Short.MIN_VALUE) {
                throw new IllegalArgumentException("Unable to represent Short.MIN_VALUE with ShortType column");
            }
        }

        @Override
        public void visit(IntType intType) {
            if (intType.castValue(in) == Integer.MIN_VALUE) {
                throw new IllegalArgumentException("Unable to represent Integer.MIN_VALUE with IntType column");
            }
        }

        @Override
        public void visit(LongType longType) {
            if (longType.castValue(in) == Long.MIN_VALUE) {
                throw new IllegalArgumentException("Unable to represent Long.MIN_VALUE with LongType column");
            }
        }

        @Override
        public void visit(FloatType floatType) {
            if (floatType.castValue(in) == -Float.MAX_VALUE) {
                throw new IllegalArgumentException("Unable to represent -Float.MAX_VALUE with FloatType column");
            }
        }

        @Override
        public void visit(DoubleType doubleType) {
            if (doubleType.castValue(in) == -Double.MAX_VALUE) {
                throw new IllegalArgumentException("Unable to represent -Double.MAX_VALUE with DoubleType column");
            }
        }

        @Override
        public void visit(StringType stringType) {

        }

        @Override
        public void visit(GenericType<?> genericType) {

        }
    }
}
