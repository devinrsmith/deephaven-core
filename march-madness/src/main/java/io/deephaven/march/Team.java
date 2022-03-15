package io.deephaven.march;

import org.immutables.value.Value.Immutable;

@Immutable
@MarchStyle
public abstract class Team {

    public abstract int id();

    public abstract String name();

    public abstract String url();
}
