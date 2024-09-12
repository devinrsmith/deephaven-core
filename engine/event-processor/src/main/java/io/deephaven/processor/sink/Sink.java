//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class Sink {
    public static Builder builder() {
        return ImmutableSink.builder();
    }

    // todo: should this "echo" EventSpec; I think _no_ b/c it doesn't _have_ to be tied to a EventProcessorFactory

    /**
     * The coordinator.
     */
    public abstract Coordinator coordinator();

    /**
     * The streams.
     */
    public abstract List<Stream> streams();

    public final Stream singleStream() {
        if (streams().size() != 1) {
            throw new IllegalStateException();
        }
        return streams().get(0);
    }

    // todo: can this sink be used _outside_ of an EventProcessor / io.deephaven.processor.EventProcessorFactory.create;
    // I think the idea is _yes_, in which case coordinator is mandatory?

    // todo: optional if user doesn't use? no; eventsink must always have coordinator, doesn't _need_ to be used w/
    // EventProcessorFactory
    // for example, can use w/ async websocket

    public interface Builder {
        Builder coordinator(Coordinator coordinator);

        Builder addStreams(Stream element);

        Builder addStreams(Stream... elements);

        Builder addAllStreams(Iterable<? extends Stream> elements);

        Sink build();
    }
}
