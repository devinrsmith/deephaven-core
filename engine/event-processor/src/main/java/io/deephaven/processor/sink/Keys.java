package io.deephaven.processor.sink;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@Value
public abstract class Keys {

    public abstract List<Key<?>> keys();
}
