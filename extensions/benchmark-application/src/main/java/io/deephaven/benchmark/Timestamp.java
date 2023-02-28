package io.deephaven.benchmark;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.time.Instant;

@Immutable
@SimpleStyle
public abstract class Timestamp implements Metric {

    public static Timestamp of(String name, Instant timestamp) {
        return ImmutableTimestamp.of(name, timestamp);
    }

    @Parameter
    public abstract String name();

    @Parameter
    public abstract Instant timestamp();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
