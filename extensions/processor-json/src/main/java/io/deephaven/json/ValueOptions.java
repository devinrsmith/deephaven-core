/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.json.jackson.JacksonProvider;
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

    /**
     * Creates a default object processor of type {@code inputType} from {@code this}. Callers wanting more control are
     * encouraged to depend on a specific implementation and construct an object processor from {@code this} more
     * explicitly.
     *
     * @param inputType the input type
     * @return the object processor
     * @param <T> the input type
     */
    @Override
    public final <T> ObjectProcessor<? super T> processor(Class<T> inputType) {
        return defaultProvider().processor(inputType);
    }

    /**
     * Creates a default named object processor of type {@code inputType} from {@code this}. Callers wanting more
     * control are encouraged to depend on a specific implementation and construct a named object processor from
     * {@code this} more explicitly.
     *
     * @param inputType the input type
     * @return the named object processor
     * @param <T> the input type
     */
    @Override
    public final <T> NamedObjectProcessor<? super T> named(Class<T> inputType) {
        return defaultProvider().named(inputType);
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

        V build();
    }

    // for nested / typedescr cases
    ValueOptions withMissingSupport() {
        if (allowMissing()) {
            return this;
        }
        throw new UnsupportedOperationException(); // todo
    }

    private JacksonProvider defaultProvider() {
        // This is the only reference from io.deephaven.json into io.deephaven.json.jackson. If we want to break out
        // io.deephaven.json.jackson into a separate project, we'd probably want a ServiceLoader pattern here to choose
        // a default implementation.
        return JacksonProvider.of(this);
    }
}
