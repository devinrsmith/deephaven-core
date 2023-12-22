/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.Function.ToInt.Plain;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class IntOptions extends ValueOptions {

    public static IntOptions of() {
        return builder().build();
    }

    public static IntOptions ofStrict() {
        return null;
        // return builder().allowNull(false).allowMissing(false).build();
    }

    public static Builder builder() {
        return null;
        // return ImmutableIntOptions.builder();
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    public abstract OptionalInt onNull();

    public abstract OptionalInt onMissing();

    // todo onNull, onMissing

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.intType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return null;
        // return new IntChunkFromNumberIntProcessor(context, allowNull(), allowMissing(),
        // out.get(0).asWritableIntChunk(),
        // onNull().orElse(QueryConstants.NULL_INT), onMissing().orElse(QueryConstants.NULL_INT), Plain.INT_VALUE);
    }

    // @Check
    // final void checkOnNull() {
    // if (!allowNull() && onNull().isPresent()) {
    // throw new IllegalArgumentException();
    // }
    // }
    //
    // @Check
    // final void checkOnMissing() {
    // if (!allowMissing() && onMissing().isPresent()) {
    // throw new IllegalArgumentException();
    // }
    // }

    public interface Builder extends ValueOptions.Builder<IntOptions, Builder> {

        Builder onNull(int onNull);

        Builder onMissing(int onMissing);
    }
}
