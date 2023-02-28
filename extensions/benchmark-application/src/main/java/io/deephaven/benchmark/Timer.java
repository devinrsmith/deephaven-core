package io.deephaven.benchmark;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.time.Duration;

@Immutable
@SimpleStyle
public abstract class Timer implements Metric {

    public static Timer of(String name, Duration duration) {
        return ImmutableTimer.of(name, duration);
    }

    @Parameter
    public abstract String name();

    @Parameter
    public abstract Duration duration();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
