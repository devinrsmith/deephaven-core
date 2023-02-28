package io.deephaven.benchmark;

public interface Metric {

    String name();

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(Timestamp timestamp);

        T visit(Timer timer);

        T visit(MultistageTimer multistageTimer);
    }
}
