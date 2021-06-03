package io.deephaven.qst;

import java.util.stream.Stream;

public interface ColumnType<T> {

    static <T> ColumnType<T> find(Class<T> clazz) {
        return ColumnTypeMappings
            .findStatic(clazz)
            .orElseGet(() -> GenericType.of(clazz));
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

    static FloatType floatType() {
        return FloatType.instance();
    }

    static DoubleType doubleType() {
        return DoubleType.instance();
    }

    static StringType stringType() {
        return StringType.instance();
    }

    static Stream<ColumnType<?>> staticTypes() {
        return Stream.of(
            booleanType(),
            intType(),
            longType(),
            floatType(),
            doubleType(),
            stringType());
    }

    static <T> T castValue(@SuppressWarnings("unused") ColumnType<T> columnType, Object value) {
        //noinspection unchecked
        return (T)value;
    }

    <V extends Visitor> V walk(V visitor);

    T castValue(Object value);

    interface Visitor {
        void visit(BooleanType booleanType);
        void visit(IntType intType);
        void visit(LongType longType);
        void visit(FloatType floatType);
        void visit(DoubleType doubleType);
        void visit(StringType stringType);
        void visit(GenericType<?> genericType);
    }
}
