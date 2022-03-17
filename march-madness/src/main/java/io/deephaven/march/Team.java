package io.deephaven.march;

import org.immutables.value.Value.Immutable;

import java.util.Comparator;

@Immutable
@MarchStyle
public abstract class Team {

    public static final Comparator<Team> SEED_COMPARATOR = Comparator.comparingInt(Team::seed);

    public static Team of(int seed, String name, String url) {
        return ImmutableTeam.builder().seed(seed).name(name).url(url).build();
    }

    public abstract int seed();

    public abstract String name();

    public abstract String url();
}
