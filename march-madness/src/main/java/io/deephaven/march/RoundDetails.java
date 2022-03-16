package io.deephaven.march;

import org.immutables.value.Value.Immutable;

import java.time.Instant;

@Immutable
@MarchStyle
public abstract class RoundDetails {

    public abstract int roundOf();

    public abstract Instant start();

    public abstract Instant endTime();
}
