//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.processor.factory.ImmutableEventProcessorSpec;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class EventProcessorSpec {
    public static Builder builder() {
        return ImmutableEventProcessorSpec.builder();
    }

    public final int numStreams() {
        return streams().size();
    }

    public abstract List<EventProcessorStreamSpec> streams();

    // TODO KEY


    // this is really tied to if the EventProcessor will use the coordinator; not whether there should or should not be
    // a coordinator
    public abstract boolean usesCoordinator(); // todo

    public interface Builder {
        Builder addStreams(EventProcessorStreamSpec element);

        Builder addStreams(EventProcessorStreamSpec... elements);

        Builder addAllStreams(Iterable<? extends EventProcessorStreamSpec> elements);

        Builder usesCoordinator(boolean usesCoordinator);

        EventProcessorSpec build();
    }

}
