package io.deephaven.march;

import org.immutables.value.Value.Immutable;

@Immutable
@MarchStyle
public abstract class Match {

    public abstract Team teamA();

    public abstract Team teamB();
}
