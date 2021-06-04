package io.deephaven.qst.table.time;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class SystemTimeProvider extends TimeProviderBase {

    public static SystemTimeProvider of() {
        return ImmutableSystemTimeProvider.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
