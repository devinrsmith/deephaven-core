package io.deephaven.qst;

class DoubleLogic extends DoubleLogicBase {

    @Override
    public final double transform(int x) {
        return x;
    }

    @Override
    public final double transform(String x) {
        return Double.parseDouble(x);
    }

    @Override
    public final <T> double transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
