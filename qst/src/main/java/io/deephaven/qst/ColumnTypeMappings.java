package io.deephaven.qst;

import io.deephaven.qst.ColumnType.Visitor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

class ColumnTypeMappings {
    private static final Map<Class<?>, ColumnType<?>> MAPPINGS;

    static {
        final AddMappings addMappings = new AddMappings();
        final Iterator<ColumnType<?>> it = ColumnType.staticTypes().iterator();
        while (it.hasNext()) {
            it.next().walk(addMappings);
        }
        MAPPINGS = Collections.unmodifiableMap(addMappings.mappings);
    }

    static <T> Optional<ColumnType<T>> findStatic(Class<T> clazz) {
        //noinspection unchecked
        return Optional.ofNullable((ColumnType<T>)MAPPINGS.get(clazz));
    }

    static class AddMappings implements Visitor {

        private final Map<Class<?>, ColumnType<?>> mappings = new HashMap<>();

        @Override
        public void visit(BooleanType booleanType) {
            mappings.put(boolean.class, booleanType);
            mappings.put(Boolean.class, booleanType);
        }

        @Override
        public void visit(IntType intType) {
            mappings.put(int.class, intType);
            mappings.put(Integer.class, intType);
        }

        @Override
        public void visit(LongType longType) {
            mappings.put(long.class, longType);
            mappings.put(Long.class, longType);
        }

        @Override
        public void visit(FloatType floatType) {
            mappings.put(float.class, floatType);
            mappings.put(Float.class, floatType);
        }

        @Override
        public void visit(DoubleType doubleType) {
            mappings.put(double.class, doubleType);
            mappings.put(Double.class, doubleType);
        }

        @Override
        public void visit(StringType stringType) {
            mappings.put(String.class, stringType);
        }

        @Override
        public void visit(GenericType<?> genericType) {

        }
    }
}
