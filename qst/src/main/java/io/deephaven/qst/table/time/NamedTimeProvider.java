package io.deephaven.qst.table.time;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

// todo: should we have a better way w/ query scopes and handles to deal w/ any variables?
@Immutable(builder = false, copy = false)
public abstract class NamedTimeProvider extends TimeProviderBase {

    public static NamedTimeProvider of(String name) {
        return ImmutableNamedTimeProvider.of(name);
    }

    @Parameter
    public abstract String name();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
