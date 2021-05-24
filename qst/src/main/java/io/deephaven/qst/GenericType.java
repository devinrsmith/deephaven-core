package io.deephaven.qst;

import java.util.Iterator;
import java.util.stream.Stream;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class GenericType<T> implements ColumnType<T> {

    public static <T> GenericType<T> of(Class<T> clazz) {
        return ImmutableGenericType.<T>builder().clazz(clazz).build();
    }

    public abstract Class<T> clazz();

    @Override
    public final Stream<Class<T>> classes() {
        return Stream.of(clazz());
    }

    @Override
    public final boolean isValidValue(T item) {
        return true; // todo
    }

    @Override
    public final Column<T> cast(Column<?> column) {
        //noinspection unchecked
        return (Column<T>) column;
    }

    @Override
    public final ColumnHeader<T> cast(ColumnHeader<?> columnHeader) {
        //noinspection unchecked
        return (ColumnHeader<T>) columnHeader;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final T castValue(Object value) {
        //noinspection unchecked
        return (T)value;
    }

    @Override
    public final <R> Iterable<T> transformValues(TypeLogic logic, ColumnType<R> fromType, Iterable<R> fromValues) {
        return () -> new Iterator<T>() {
            private final Iterator<R> it = fromValues.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return logic.transform(GenericType.this, fromType, it.next());
            }
        };
    }

    @Check
    final void checkClazz() {
        for (ColumnType<?> type : ColumnType.STATICS) {
            if (type.classes().anyMatch(clazz()::equals)) {
                throw new IllegalArgumentException(String.format("May not use type %s as %s, see %s", clazz(), GenericType.class, type.getClass()));
            }
        }
    }
}
