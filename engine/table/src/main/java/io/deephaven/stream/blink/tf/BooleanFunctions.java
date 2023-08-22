package io.deephaven.stream.blink.tf;

class BooleanFunctions {

    public static <T> BooleanFunction<T> ofTrue() {
        // noinspection unchecked
        return (BooleanFunction<T>) OfTrue.INSTANCE;
    }

    public static <T> BooleanFunction<T> ofFalse() {
        // noinspection unchecked
        return (BooleanFunction<T>) OfFalse.INSTANCE;
    }

    enum OfTrue implements BooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean applyAsBoolean(Object value) {
            return true;
        }
    }

    enum OfFalse implements BooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean applyAsBoolean(Object value) {
            return false;
        }
    }
}
