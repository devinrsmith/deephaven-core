//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Sink;

import java.util.List;

public interface EventProcessorFactory<T> {

    // the user-defined output types(s)
    // EventProcessorSpec spec();

    // Map<StreamKey, EventProcessorStreamSpec> specs();
    List<EventProcessorStreamSpec> specs();

    // creates an event processor, with sink types aligning with spec.
    // should be thread safe. caller must close when done.
    // todo: map<StreamKey, EventProcessorStreamSpec>?

    /**
     * Creates an event processor.
     * 
     * @param sink
     * @return
     */
    EventProcessor<T> create(Sink sink);

    interface EventProcessor<T> extends AutoCloseable {

        /**
         * Write {@code event} to the sink.
         *
         * <p>
         * The implementation of this method is meant to be "cheap" since the execution of this method may block
         * downstream consumers from proceeding.
         *
         * <p>
         * In the case where this method in aggregate is "expensive" (for example, it may be parsing a JSON array of
         * indeterminate length) implementations are advised to {@link Coordinator#yield() yield} at appropriate
         * boundaries.
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
