/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.json.ArrayOptions;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class ArrayProcessorBase<T> implements ArrayProcessor {

    public abstract class ArrayContextBase implements Context {
        @Override
        public final boolean hasElement(JsonParser parser) {
            return !parser.hasToken(JsonToken.END_ARRAY);
        }

        @Override
        public final void done(JsonParser parser) throws IOException {
            consumer.accept(onDone());
        }

        public abstract T onDone();
    }

    private final ArrayOptions options;
    private final Consumer<? super T> consumer;
    private final T onMissing;
    private final T onNull;

    public ArrayProcessorBase(ArrayOptions options, Consumer<? super T> consumer, T onMissing, T onNull) {
        this.options = Objects.requireNonNull(options);
        this.consumer = Objects.requireNonNull(consumer);
        this.onMissing = onMissing;
        this.onNull = onNull;
    }


    public abstract ArrayContextBase newContext();

    @Override
    public final Context start(JsonParser parser) throws IOException {
        Helpers.assertCurrentToken(parser, JsonToken.START_ARRAY);
        parser.nextToken();
        return newContext();
    }

    @Override
    public final void processMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, void.class);
        }
        consumer.accept(onMissing);
    }

    @Override
    public final void processNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, void.class);
        }
        consumer.accept(onNull);
    }
}
