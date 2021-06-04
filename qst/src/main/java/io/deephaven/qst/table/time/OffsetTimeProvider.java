package io.deephaven.qst.table.time;

import java.time.Duration;
import java.util.Optional;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class OffsetTimeProvider extends TimeProviderBase {

    public abstract TimeProvider parent();

    public abstract Optional<Duration> offset(); // if no offset provided, offset is based on firstTime()

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
