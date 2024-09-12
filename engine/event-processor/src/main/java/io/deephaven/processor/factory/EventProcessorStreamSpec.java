//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.stream.Collectors;


@Immutable
@BuildableStyle
public abstract class EventProcessorStreamSpec {

    public static Builder builder() {
        return ImmutableEventProcessorStreamSpec.builder();
    }

    public static final class Key<T> {
        private final Type<T> type;
        private final String name;

        public Key(String debugName, Type<T> type) {
            this.type = Objects.requireNonNull(type);
            this.name = Objects.requireNonNull(debugName);
        }

        public Type<T> type() {
            return type;
        }

        @Override
        public String toString() {
            return name;
        }
    }


    // todo: give ability to mark as 1 to 1

    @Deprecated // todo
    public final List<Type<?>> outputTypes() {
        return keys().stream().map(Key::type).collect(Collectors.toList());
    }

    public abstract List<Key<?>> keys(); // todo

    // yeah, this is more of a event factory propertie?
    public abstract OptionalLong expectedSize();

    // if true, caller must use Stream.advanceAll() instead of advance()
    // todo: this is more tied with EventProcessorSpec than stream? can place restrictions on API used (ie, can't use
    // advance, must use advanceAll, etc)
    public abstract boolean isRowOriented();

    public interface Builder {

        Builder addKeys(Key<?> element);

        Builder addKeys(Key<?>... elements);

        Builder addAllKeys(Iterable<? extends Key<?>> elements);

//        Builder addOutputTypes(Type<?> element);
//
//        Builder addOutputTypes(Type<?>... elements);
//
//        Builder addAllOutputTypes(Iterable<? extends Type<?>> elements);

        Builder expectedSize(long expectedSize);

        Builder isRowOriented(boolean isRowOriented);

        EventProcessorStreamSpec build();
    }
}
