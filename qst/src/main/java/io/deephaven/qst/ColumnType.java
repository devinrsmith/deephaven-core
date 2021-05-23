package io.deephaven.qst;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public interface ColumnType<T> {

    List<ColumnType<?>> STATICS = Arrays.asList(
      IntType.instance(),
      StringType.instance(),
      DoubleType.instance());

    static <T> ColumnType<T> find(Class<T> clazz) {
        // todo: better
        for (ColumnType<?> type : STATICS) {
            if (type.classes().anyMatch(clazz::equals)) {
                //noinspection unchecked
                return (ColumnType<T>)type;
            }
        }
        return GenericType.of(clazz);
    }

    Stream<Class<T>> classes();

    boolean isValidValue(T item);

    <V extends Visitor> V walk(V visitor);

    Column<T> cast(Column<?> column);

    interface Visitor {
        void visit(IntType intType);
        void visit(StringType stringType);
        void visit(DoubleType doubleType);
        void visit(GenericType<?> genericType);
    }
}
