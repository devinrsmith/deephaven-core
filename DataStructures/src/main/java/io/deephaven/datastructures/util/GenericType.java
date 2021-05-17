package io.deephaven.datastructures.util;

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
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
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
