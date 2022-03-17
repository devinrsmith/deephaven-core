package io.deephaven.march;

import org.immutables.value.Value.Immutable;

import java.time.Instant;
import java.util.Optional;

@Immutable
@MarchStyle
public abstract class Vote {

    public abstract Instant timestamp();

    public abstract String ip();

    public abstract long session();

    public abstract Optional<String> userAgent();

    public abstract int roundOf();

    public abstract int teamId();
}
