package io.deephaven.plugin.app;

public abstract class ConsumerBase implements State.Consumer {

    @Override
    public final void setState(String prefix, State state) {
        state.insertInto(new PrefixedConsumer(prefix, this));
    }
}
