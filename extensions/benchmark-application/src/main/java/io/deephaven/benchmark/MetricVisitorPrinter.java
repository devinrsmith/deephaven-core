package io.deephaven.benchmark;

public enum MetricVisitorPrinter implements Metric.Visitor<Void> {
    INSTANCE;

    @Override
    public Void visit(Timestamp timestamp) {
        System.out.println(timestamp);
        return null;
    }

    @Override
    public Void visit(Timer timer) {
        System.out.println(timer);
        return null;
    }

    @Override
    public Void visit(MultistageTimer multistageTimer) {
        System.out.println(multistageTimer);
        return null;
    }
}
