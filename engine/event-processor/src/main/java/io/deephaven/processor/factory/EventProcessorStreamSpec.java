//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.processor.factory.ImmutableEventProcessorStreamSpec;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.OptionalLong;


@Immutable
@BuildableStyle
public abstract class EventProcessorStreamSpec {

    public static Builder builder() {
        return ImmutableEventProcessorStreamSpec.builder();
    }


    // todo: give ability to mark as 1 to 1

    public abstract List<Type<?>> outputTypes();

    // yeah, this is more of a event factory propertie?
    public abstract OptionalLong expectedSize();

    // if true, caller must use Stream.advanceAll() instead of advance()
    // todo: this is more tied with EventProcessorSpec than stream? can place restrictions on API used (ie, can't use
    // advance, must use advanceAll, etc)
    public abstract boolean isRowOriented();

    public interface Builder {

        Builder addOutputTypes(Type<?> element);

        Builder addOutputTypes(Type<?>... elements);

        Builder addAllOutputTypes(Iterable<? extends Type<?>> elements);

        Builder expectedSize(long expectedSize);

        Builder isRowOriented(boolean isRowOriented);

        EventProcessorStreamSpec build();
    }
}
