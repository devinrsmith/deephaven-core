/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.json.jackson.JacksonConfiguration;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import org.immutables.value.Value.Default;

public abstract class ValueOptions implements ObjectProcessor.Provider, NamedObjectProcessor.Provider {


    @Default
    public boolean allowNull() {
        return true;
    }

    @Default
    public boolean allowMissing() {
        return true;
    }

    @Default
    public JsonConfiguration jsonConfiguration() {
        return JacksonConfiguration.defaultInstance();
    }


    /**
     * Equivalent to {@code jsonConfiguration().namedProvider(this).named(inputType)}.
     *
     * @param inputType the input type
     * @return
     * @param <T>
     * @see #jsonConfiguration()
     */
    @Override
    public final <T> NamedObjectProcessor<? super T> named(Class<T> inputType) {
        return jsonConfiguration().namedProvider(this).named(inputType);
    }

    /**
     * Equivalent to {@code jsonConfiguration().processorProvider(this).processor(inputType)}.
     *
     * @param inputType the input type
     * @return
     * @param <T>
     * @see #jsonConfiguration()
     */
    @Override
    public final <T> ObjectProcessor<? super T> processor(Class<T> inputType) {
        return jsonConfiguration().processorProvider(this).processor(inputType);
    }

    public final ArrayOptions toArrayOptions() {
        return null;
        // return ArrayOptions.builder()
        // .element(this)
        // .build();
    }

    public abstract <T> T walk(Visitor<T> visitor);

    public interface Visitor<T> {

        T visit(StringOptions _string);

        T visit(IntOptions _int);

        T visit(LongOptions _long);

        T visit(FloatOptions _float);

        T visit(DoubleOptions _double);

        T visit(ObjectOptions object);

        T visit(InstantOptions instant);

        T visit(InstantNumberOptions instantNumber);

        T visit(BigIntegerOptions bigInteger);

        T visit(BigDecimalOptions bigDecimal);

        T visit(SkipOptions skip);

        T visit(TupleOptions tuple);

        T visit(TypedObjectOptions typedObject);

        T visit(LocalDateOptions localDate);

        T visit(ArrayOptions array);

        T visit(AnyOptions any);
    }

    public interface Builder<V extends ValueOptions, B extends Builder<V, B>> {

        B allowNull(boolean allowNull);

        B allowMissing(boolean allowMissing);

        B jsonConfiguration(JsonConfiguration jsonConfiguration);

        V build();
    }

    // for nested / typedescr cases
    ValueOptions withMissingSupport() {
        if (allowMissing()) {
            return this;
        }
        throw new UnsupportedOperationException(); // todo
    }
}
