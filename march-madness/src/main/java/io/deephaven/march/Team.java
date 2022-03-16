package io.deephaven.march;

import org.immutables.value.Value.Immutable;

@Immutable
@MarchStyle
public abstract class Team {

    public static Team of(int seed, String name, String url) {
        return ImmutableTeam.builder().seed(seed).name(name).url(url).build();
    }

    public abstract int seed();

    public abstract String name();

    public abstract String url();
}
