package io.deephaven.qst;

class IntLogicLax extends IntLogicBase {

    @Override
    public final int transform(double x) {
        return (int)x;
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
