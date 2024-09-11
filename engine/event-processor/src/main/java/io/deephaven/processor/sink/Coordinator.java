//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.processor.sink.appender.Appender;

public interface Coordinator {

    // todo: more explicit lock? Txn w/ commit / abort?


    void writing();

    /**
     * Marks a "synchronization point". A synchronization point communicates that all {@link Stream streams} of the
     * {@link Sink} are "consistent" and that the {@link Sink} is "deliverable". A consistent stream is one in which all
     * of its {@link Appender appenders} have been written to and advanced the same amount. A deliverable sink is
     * context dependant, but typically means that some natural boundaries or delimiters on the underlying data have
     * been reached. This may involve intra-stream or inter-stream dependencies.
     */
    void sync();

    default void intermediate() {
        sync();
        writing();
    }

    // todo: should we be able to "sync" single streams at a time? probably not? alternatively, we can call this
    // "syncAll", and add Stream#sync?

    // void error(Throwable t);
}
