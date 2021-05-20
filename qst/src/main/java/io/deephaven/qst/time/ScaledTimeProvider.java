package io.deephaven.qst.time;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false)
public abstract class ScaledTimeProvider extends TimeProviderBase {

    @Parameter
    public abstract TimeProvider parent();

    @Parameter
    public abstract double scale();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
