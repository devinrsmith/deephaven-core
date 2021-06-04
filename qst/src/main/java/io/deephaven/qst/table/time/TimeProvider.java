package io.deephaven.qst.table.time;

import java.time.Duration;

public interface TimeProvider {

    static SystemTimeProvider system() {
        return SystemTimeProvider.of();
    }

    static NamedTimeProvider named(String name) {
        return NamedTimeProvider.of(name);
    }

    OffsetTimeProvider offset(Duration offset);

    OffsetTimeProvider normalized();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(SystemTimeProvider system);
        void visit(NamedTimeProvider named);
        void visit(OffsetTimeProvider offset);
        void visit(ScaledTimeProvider scaled);
    }
}
