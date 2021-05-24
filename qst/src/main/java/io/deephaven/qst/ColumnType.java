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

    static BooleanType booleanType() {
        return BooleanType.instance();
    }

    static IntType intType() {
        return IntType.instance();
    }

    static LongType longType() {
        return LongType.instance();
    }

    static DoubleType doubleType() {
        return DoubleType.instance();
    }

    static StringType stringType() {
        return StringType.instance();
    }

    Stream<Class<T>> classes();

    boolean isValidValue(T item);

    <V extends Visitor> V walk(V visitor);

    Column<T> cast(Column<?> column);

    ColumnHeader<T> cast(ColumnHeader<?> columnHeader);

    T castValue(Object value);

    <R> Iterable<T> transformValues(TypeLogic logic, ColumnType<R> fromType, Iterable<R> fromValues);

    interface Visitor {
        void visit(BooleanType booleanType);
        void visit(IntType intType);
        void visit(LongType longType);
        void visit(StringType stringType);
        void visit(DoubleType doubleType);
        void visit(GenericType<?> genericType);
    }
}
