/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class ArrayProcessorBase<T> implements ArrayProcessor {

    private final Consumer<? super T> consumer;
    private final boolean allowMissing;
    private final boolean allowNull;
    private final T onMissing;
    private final T onNull;

    public ArrayProcessorBase(Consumer<? super T> consumer, boolean allowMissing, boolean allowNull, T onMissing,
            T onNull) {
        this.consumer = Objects.requireNonNull(consumer);
        this.onMissing = onMissing;
        this.onNull = onNull;
        this.allowNull = allowNull;
        this.allowMissing = allowMissing;
    }

    public abstract ArrayContextBase newContext();

    @Override
    public final Context start(JsonParser parser) throws IOException {
        Helpers.assertCurrentToken(parser, JsonToken.START_ARRAY);
        return newContext();
    }

    @Override
    public final void processMissingArray(JsonParser parser) throws IOException {
        if (!allowMissing) {
            throw Helpers.mismatchMissing(parser, void.class);
        }
        consumer.accept(onMissing);
    }

    @Override
    public final void processNullArray(JsonParser parser) throws IOException {
        if (!allowNull) {
            throw Helpers.mismatch(parser, void.class);
        }
        consumer.accept(onNull);
    }

    public abstract class ArrayContextBase implements Context {
        @Override
        public final boolean hasElement(JsonParser parser) {
            return !parser.hasToken(JsonToken.END_ARRAY);
        }

        @Override
        public final void done(JsonParser parser, int length) throws IOException {
            consumer.accept(onDone(length));
        }

        public abstract T onDone(int length);
    }
}
