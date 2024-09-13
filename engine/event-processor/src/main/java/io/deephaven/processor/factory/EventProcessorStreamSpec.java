//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.processor.factory.ImmutableEventProcessorStreamSpec.Builder;
import io.deephaven.processor.sink.Key;
import io.deephaven.processor.sink.Keys;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Sink.StreamKey;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;


@Immutable
@BuildableStyle
public abstract class EventProcessorStreamSpec {

    public static Builder builder() {
        return ImmutableEventProcessorStreamSpec.builder();
    }

    public abstract StreamKey key();

    public abstract boolean usesCoordinator();

    // todo: give ability to mark as 1 to 1

    // @Deprecated // todo
    // public final List<Type<?>> outputTypes() {
    // return keys().stream().map(Key::type).collect(Collectors.toList());
    // }

    public abstract Keys keys(); // todo

    // yeah, this is more of a event factory propertie?
    public abstract OptionalLong expectedSize();

    // if true, caller must use Stream.advanceAll() instead of advance()
    // todo: this is more tied with EventProcessorSpec than stream? can place restrictions on API used (ie, can't use
    // advance, must use advanceAll, etc)
    public abstract boolean isRowOriented();

    public interface Builder {
        Builder key(StreamKey key);

        Builder keys(Keys keys);

        Builder usesCoordinator(boolean usesCoordinator);


        // Builder addKeys(Key<?> element);
        //
        // Builder addKeys(Key<?>... elements);
        //
        // Builder addAllKeys(Iterable<? extends Key<?>> elements);

        // Builder addOutputTypes(Type<?> element);
        //
        // Builder addOutputTypes(Type<?>... elements);
        //
        // Builder addAllOutputTypes(Iterable<? extends Type<?>> elements);

        Builder expectedSize(long expectedSize);

        Builder isRowOriented(boolean isRowOriented);

        EventProcessorStreamSpec build();
    }
}
