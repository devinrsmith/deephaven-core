package io.deephaven.engine.table.impl.perf;

import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

public interface SomeHotspotThing {

    static Optional<SomeHotspotThing> loadImpl() {
        final Iterator<SomeHotspotThing> it = ServiceLoader.load(SomeHotspotThing.class).iterator();
        if (!it.hasNext()) {
            return Optional.empty();
        }
        final SomeHotspotThing impl = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException("Found multiple impls");
        }
        return Optional.of(impl);
    }

    long getSafepointCount();
}
