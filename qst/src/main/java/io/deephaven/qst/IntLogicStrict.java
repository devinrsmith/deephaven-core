package io.deephaven.qst;

public class IntLogicStrict extends IntLogicBase {
    private static final IntLogicStrict INSTANCE = new IntLogicStrict();

    public static IntLogicStrict instance() {
        return INSTANCE;
    }

    private IntLogicStrict() {
    }

    @Override
    public final int transform(double x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final int transform(String x) {
        return Integer.parseInt(x);
    }

    @Override
    public final <T> int transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
