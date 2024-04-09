//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The base configuration for JSON values.
 */
public abstract class ValueOptions implements ObjectProcessor.Provider, NamedObjectProcessor.Provider {

    /**
     * The allowed types.
     */
    public abstract EnumSet<JsonValueTypes> allowedTypes();

    /**
     * If the processor should allow a missing JSON value. By default is {@code true}.
     */
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
    public final <T> ObjectProcessor<? super T> processor(Type<T> inputType) {
        return JsonProcessorProvider.serviceLoader().provider(this).processor(inputType);
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
    public final <T> NamedObjectProcessor<? super T> named(Type<T> inputType) {
        return JsonProcessorProvider.serviceLoader().namedProvider(this).named(inputType);
    }

    public final SkipOptions skip() {
        return SkipOptions.builder()
                .allowMissing(allowMissing())
                .allowedTypes(allowedTypes())
                .build();
    }

    public final ArrayOptions array() {
        return ArrayOptions.standard(this);
    }

    public final ObjectOptions field(String name) {
        return ObjectOptions.standard(Map.of(name, this));
    }

    public abstract <T> T walk(Visitor<T> visitor);

    public interface Visitor<T> {

        T visit(StringOptions _string);

        T visit(BoolOptions _bool);

        T visit(CharOptions _char);

        T visit(ByteOptions _byte);

        T visit(ShortOptions _short);

        T visit(IntOptions _int);

        T visit(LongOptions _long);

        T visit(FloatOptions _float);

        T visit(DoubleOptions _double);

        T visit(ObjectOptions object);

        T visit(ObjectKvOptions objectKv);

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

        B allowMissing(boolean allowMissing);

        B allowedTypes(EnumSet<JsonValueTypes> allowedTypes);

        default B allowedTypes(JsonValueTypes... allowedTypes) {
            final EnumSet<JsonValueTypes> set = EnumSet.noneOf(JsonValueTypes.class);
            set.addAll(Arrays.asList(allowedTypes));
            return allowedTypes(set);
        }

        V build();
    }

    abstract EnumSet<JsonValueTypes> restrictedToTypes();

    @Check
    void checkIllegalTypes() {
        for (JsonValueTypes type : allowedTypes()) {
            if (!restrictedToTypes().contains(type)) {
                throw new IllegalArgumentException("Unexpected type " + type);
            }
        }
    }

    @Check
    final void checkInvariants() {
        JsonValueTypes.checkInvariants(allowedTypes());
    }
}
