/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.BigDecimalOptions;
import io.deephaven.json.BigIntegerOptions;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.FloatOptions;
import io.deephaven.json.InstantNumberOptions;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.IntOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.ValueOptions;

import java.util.List;
import java.util.Objects;

final class ProcessorImpl implements ValueOptions.Visitor<ValueProcessor> {

    public static ValueProcessor of(ValueOptions options, String context, List<WritableChunk<?>> out) {
        return options.walk(new ProcessorImpl(context, out));
    }

    private final String context;
    private final List<WritableChunk<?>> out;

    public ProcessorImpl(String context, List<WritableChunk<?>> out) {
        this.context = Objects.requireNonNull(context);
        this.out = Objects.requireNonNull(out);
    }

    @Override
    public ValueProcessor visit(IntOptions _int) {
        return null;
    }

    @Override
    public ValueProcessor visit(LongOptions _long) {
        return null;
    }

    @Override
    public ValueProcessor visit(FloatOptions _float) {
        return null;
    }

    @Override
    public ValueProcessor visit(DoubleOptions _double) {
        return null;
    }

    @Override
    public ValueProcessor visit(ObjectOptions object) {
        return new ObjectOptionsImpl(object).processor(context, out);
    }

    @Override
    public ValueProcessor visit(InstantOptions instant) {
        return null;
    }

    @Override
    public ValueProcessor visit(InstantNumberOptions instantNumber) {
        return null;
    }

    @Override
    public ValueProcessor visit(BigIntegerOptions bigInteger) {
        return null;
    }

    @Override
    public ValueProcessor visit(BigDecimalOptions bigDecimal) {
        return null;
    }
}
