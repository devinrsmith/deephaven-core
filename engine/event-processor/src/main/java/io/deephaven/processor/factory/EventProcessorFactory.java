//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Sink;

public interface EventProcessorFactory<T> {

    // the user-defined output types(s)
    EventProcessorSpec spec();

    // creates an event processor, with sink types aligning with spec.
    // should be thread safe. caller must close when done.

    /**
     * Creates an event processor.
     * 
     * @param sink
     * @return
     */
    EventProcessor<T> create(Sink sink);

    interface EventProcessor<T> extends AutoCloseable {

        /**
         * Write {@code event} to the sink. The entrance of this method implicitly marks a
         * {@link Coordinator#writing()}; the successful return of this method implicitly marks a
         * {@link Coordinator#sync() synchronization point}.
         * 
         * @param event the event
         */
        void writeToSink(T event);

        // todo: allow arrays? the challenge would be to additionally need to communicate offset via Sink
        // void acceptAll(T[] events, int offset, int length);

        // todo: allow exceptions?
        @Override
        void close();
    }
}
