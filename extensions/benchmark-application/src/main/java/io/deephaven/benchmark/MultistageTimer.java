package io.deephaven.benchmark;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Map;

@Immutable
@BuildableStyle
public abstract class MultistageTimer implements Metric {

    public static Builder builder() {
        return ImmutableMultistageTimer.builder();
    }

    public abstract String name();

    public abstract Map<String, Duration> stages();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    interface Builder {
        Builder name(String name);

        Builder putStages(String key, Duration value);

        Builder putAllStages(Map<String, ? extends Duration> entries);

        MultistageTimer build();
    }
}
