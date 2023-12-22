/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

class ArrayValueProcessorImpl<V extends ValueProcessor> extends ArrayValueProcessorBase<V> {

    private final Supplier<V> supplier;
    private final Consumer<V> consumer;

    public ArrayValueProcessorImpl(String contextPrefix, boolean allowNull, boolean allowMissing, Supplier<V> supplier,
            Consumer<V> consumer) {
        super(contextPrefix, allowNull, allowMissing);
        this.supplier = supplier;
        this.consumer = consumer;
    }

    @Override
    protected V start() {
        return supplier.get();
    }

    @Override
    protected void end(V valueProcessor) {
        consumer.accept(valueProcessor);
    }


    @Override
    protected void handleNull(JsonParser parser) throws IOException {
        // same as empty array
        consumer.accept(supplier.get());
    }

    @Override
    protected void handleMissing(JsonParser parser) throws IOException {
        // same as empty array
        consumer.accept(supplier.get());
    }
}
