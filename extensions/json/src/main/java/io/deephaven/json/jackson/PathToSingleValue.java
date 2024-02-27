/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.json.AnyOptions;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.BigDecimalOptions;
import io.deephaven.json.BigIntegerOptions;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.FloatOptions;
import io.deephaven.json.InstantNumberOptions;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.IntOptions;
import io.deephaven.json.LocalDateOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.SkipOptions;
import io.deephaven.json.StringOptions;
import io.deephaven.json.TupleOptions;
import io.deephaven.json.TypedObjectOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.ValueOptions.Visitor;
import io.deephaven.json.jackson.PathToSingleValue.Results;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

final class PathToSingleValue implements Visitor<Results> {

    @Immutable
    @BuildableStyle
    static abstract class Results {

        public abstract List<String> path();

        public abstract ValueOptions options();
    }

    public static Results of(ValueOptions options) {
        return Objects.requireNonNull(options.walk(new PathToSingleValue()));
    }

    private final ImmutableResults.Builder builder = ImmutableResults.builder();

    @Override
    public Results visit(ObjectOptions object) {
        if (object.fields().size() != 1) {
            return complete(object);
        }
        final Entry<String, ValueOptions> entry = object.fields().entrySet().iterator().next();
        builder.addPath(entry.getKey());
        return entry.getValue().walk(this);
    }

    @Override
    public Results visit(ArrayOptions array) {
        return complete(array);
    }

    @Override
    public Results visit(StringOptions _string) {
        return complete(_string);
    }

    @Override
    public Results visit(IntOptions _int) {
        return complete(_int);
    }

    @Override
    public Results visit(LongOptions _long) {
        return complete(_long);
    }

    @Override
    public Results visit(FloatOptions _float) {
        return complete(_float);
    }

    @Override
    public Results visit(DoubleOptions _double) {
        return complete(_double);
    }

    @Override
    public Results visit(InstantOptions instant) {
        return complete(instant);
    }

    @Override
    public Results visit(InstantNumberOptions instantNumber) {
        return complete(instantNumber);
    }

    @Override
    public Results visit(BigIntegerOptions bigInteger) {
        return complete(bigInteger);
    }

    @Override
    public Results visit(BigDecimalOptions bigDecimal) {
        return complete(bigDecimal);
    }

    @Override
    public Results visit(SkipOptions skip) {
        return complete(skip);
    }

    @Override
    public Results visit(TupleOptions tuple) {
        return complete(tuple);
    }

    @Override
    public Results visit(TypedObjectOptions typedObject) {
        return complete(typedObject);
    }

    @Override
    public Results visit(LocalDateOptions localDate) {
        return complete(localDate);
    }

    @Override
    public Results visit(AnyOptions any) {
        return complete(any);
    }

    private Results complete(ValueOptions options) {
        return builder.options(options).build();
    }
}
