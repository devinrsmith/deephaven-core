package io.deephaven.plugin.app;

import java.util.Objects;

final class PrefixedConsumer extends ConsumerBase {
    private final String prefix;
    private final State.Consumer delegate;

    public PrefixedConsumer(String prefix, State.Consumer delegate) {
        this.prefix = Objects.requireNonNull(prefix);
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public void set(String name, Object object) {
        delegate.set(prefix + name, object);
    }

    @Override
    public void set(String name, Object object, String description) {
        delegate.set(prefix + name, object, description);
    }
}
