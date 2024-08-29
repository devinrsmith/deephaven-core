package io.deephaven.processor;

import java.util.List;
import java.util.Objects;

public abstract class EventProcessorSimpleSingleStreamBase<T> implements EventProcessor<T> {
    private final StreamSpec streamSpec;

    public EventProcessorSimpleSingleStreamBase(StreamSpec streamSpec) {
        this.streamSpec = Objects.requireNonNull(streamSpec);
    }

    @Override
    public final EventSpec spec() {
        return new EventSpec() {            @Override
            public int numStreams() {
                return 1;
            }

            @Override
            public List<StreamSpec> streams() {
                return List.of(streamSpec);
            }

            @Override
            public boolean usesCoordinator() {
                return false;
            }
        };
    }

    @Override
    public final void process(T event, List<Stream> streams, Coordinator coordinator) {
        processStream(event, streams.get(0));
    }

    protected abstract void processStream(T event, Stream stream);
}
