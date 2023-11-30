/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class ObjectOptions extends ValueOptions {

    public enum RepeatedFieldBehavior {
        USE_FIRST, USE_LAST, ERROR
    }

    public static Builder builder() {
        return ImmutableObjectOptions.builder();
    }

    public abstract Map<String, ValueOptions> fieldProcessors();

    @Override
    @Default
    public boolean allowNull() {
        return true;
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    /**
     * To be more selective, individual fields can be added with {@link ValueOptions#skip()} to
     * {@link #fieldProcessors()}.
     *
     * @return if unknown fields are allowed for {@code this} object
     */
    @Default
    public boolean allowUnknownFields() {
        // todo: what is the better default?
        // true is more lenient
        // false is "safer", but may cause crashes if the protocol is updated and new fields added
        // todo: we could output a column w/ the number of unknown fields / names?
        return true;
    }

    @Default
    public RepeatedFieldBehavior repeatedFieldBehavior() {
        return RepeatedFieldBehavior.USE_FIRST;
    }

    @Override
    final Map<JsonToken, JsonToken> startEndTokens() {
        return Map.of(JsonToken.START_OBJECT, JsonToken.END_ARRAY);
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return fieldProcessors().values().stream().flatMap(ValueOptions::outputTypes);
    }

    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final Map<String, ValueProcessor> processors = new LinkedHashMap<>(fieldProcessors().size());
        int ix = 0;
        for (Entry<String, ValueOptions> e : fieldProcessors().entrySet()) {
            final String fieldName = e.getKey();
            final ValueOptions opts = e.getValue();
            final int numTypes = opts.numColumns();
            final ValueProcessor fieldProcessor =
                    opts.processor(context + "/" + fieldName, out.subList(ix, ix + numTypes));
            processors.put(fieldName, fieldProcessor);
            ix += numTypes;
        }
        if (ix != out.size()) {
            throw new IllegalStateException();
        }
        return new ObjectValueFieldProcessor(
                context,
                allowNull(),
                allowMissing(),
                processors,
                allowUnknownFields() ? ValueProcessor.skip() : null,
                repeatedFieldBehavior() == RepeatedFieldBehavior.USE_FIRST ? ValueProcessor.skip() : null);
    }

    public interface Builder extends ValueOptions.Builder<ObjectOptions, Builder> {
        Builder allowUnknownFields(boolean allowUnknownFields);

        Builder repeatedFieldBehavior(RepeatedFieldBehavior repeatedFieldBehavior);

        Builder putFieldProcessors(String key, ValueOptions value);

        Builder putFieldProcessors(Map.Entry<String, ? extends ValueOptions> entry);

        Builder putAllFieldProcessors(Map<String, ? extends ValueOptions> entries);
    }
}
