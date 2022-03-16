package io.deephaven.march;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
@MarchStyle
public abstract class Vote {
    public abstract Round round();

    public abstract Team team();

    public int matchIndex() {
        return round().matchIndex(team());
    }

    @Check
    final void checkTeam() {
        if (!round().hasTeam(team())) {
            throw new IllegalArgumentException();
        }
    }
}
