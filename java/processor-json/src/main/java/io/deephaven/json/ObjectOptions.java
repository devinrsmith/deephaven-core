/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static io.deephaven.json.Helpers.assertCurrentToken;

@Immutable
@BuildableStyle
public abstract class ObjectOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableObjectOptions.builder();
    }

    public static ObjectOptions of(Map<String, ValueOptions> fields) {
        return builder().putAllFields(fields).build();
    }

    public abstract Map<String, ValueOptions> fields();

    /**
     * ...
     *
     * <p>
     * If the caller wants to throw an error on unknown fields, but knows there are fields they want to skip, the can be
     * e todo To be more selective, individual fields can be added with {@link SkipOptions} ... todoeueou
     * {@link #fields()}.
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

    // @Override
    // public final boolean allowNull() {
    // return fieldProcessors().values().stream().allMatch(ValueOptions::allowNull);
    // }
    //
    // @Override
    // public final boolean allowMissing() {
    // return fieldProcessors().values().stream().allMatch(ValueOptions::allowMissing);
    // }

    public final SkipOptions skip() {
        // todo: this doesn't make sense on this object
        return SkipOptions.builder()
                .allowObject(true)
                .allowNull(allowNull())
                .allowMissing(allowMissing())
                .build();
    }

    // Note: Builder does not extend ValueOptions.Builder b/c allowNull / allowMissing is implicitly set

    public enum RepeatedFieldBehavior {
        /**
         * Throws an error if a repeated field is encountered
         */
        ERROR,

        /**
         * Uses the first field of a given name, ignores the rest
         */
        USE_FIRST,

        // /**
        // * Uses the last field of a given name, ignores the rest. Not currently supported.
        // */
        // USE_LAST
    }

    public interface Builder extends ValueOptions.Builder<ObjectOptions, Builder> {

        // python needs these overloaded...

        @Override
        Builder allowNull(boolean allowNull);

        @Override
        Builder allowMissing(boolean allowMissing);

        Builder allowUnknownFields(boolean allowUnknownFields);

        Builder repeatedFieldBehavior(RepeatedFieldBehavior repeatedFieldBehavior);

        Builder putFields(String key, ValueOptions value);

        Builder putFields(Map.Entry<String, ? extends ValueOptions> entry);

        Builder putAllFields(Map<String, ? extends ValueOptions> entries);
    }

    @Override
    final int outputCount() {
        return fields().values().stream().mapToInt(ValueOptions::outputCount).sum();
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return fields().values().stream().flatMap(ValueOptions::outputTypes);
    }

    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final Map<String, ValueProcessor> processors = new LinkedHashMap<>(fields().size());
        int ix = 0;
        for (Entry<String, ValueOptions> e : fields().entrySet()) {
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
        return new ObjectValueFieldProcessor(processors);
    }

    private class ObjectValueFieldProcessor implements ValueProcessor {
        private final Map<String, ValueProcessor> fieldProcessors;

        ObjectValueFieldProcessor(Map<String, ValueProcessor> fieldProcessors) {
            this.fieldProcessors = Objects.requireNonNull(fieldProcessors);
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            // see com.fasterxml.jackson.databind.JsonDeserializer.deserialize(com.fasterxml.jackson.core.JsonParser,
            // com.fasterxml.jackson.databind.DeserializationContext)
            // for notes on FIELD_NAME
            switch (parser.currentToken()) {
                case START_OBJECT:
                    if (parser.nextToken() == JsonToken.END_OBJECT) {
                        processEmptyObject(parser);
                        return;
                    }
                    if (parser.currentToken() != JsonToken.FIELD_NAME) {
                        throw new IllegalStateException();
                    }
                    // fall-through
                case FIELD_NAME:
                    processObjectFields(parser);
                    return;
                case VALUE_NULL:
                    processNullObject(parser);
                    return;
                default:
                    throw Helpers.mismatch(parser, Object.class);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Helpers.mismatchMissing(parser, Object.class);
            }
            for (ValueProcessor value : fieldProcessors.values()) {
                value.processMissing(parser);
            }
        }

        private void processNullObject(JsonParser parser) throws IOException {
            if (!allowNull()) {
                throw Helpers.mismatch(parser, Object.class);
            }
            for (ValueProcessor value : fieldProcessors.values()) {
                value.processCurrentValue(parser);
            }
        }

        private void processEmptyObject(JsonParser parser) throws IOException {
            // This logic should be equivalent to processObjectFields, but where we know there are no fields
            for (ValueProcessor value : fieldProcessors.values()) {
                value.processMissing(parser);
            }
        }

        private void processObjectFields(JsonParser parser) throws IOException {
            // Note: we could try to build a stricter implementation that doesn't use Set; if the user can guarantee
            // that none of the fields will be missing and there won't be any repeated fields, we could use a simple
            // counter to ensure all field processors were invoked.
            final Set<String> visited = new HashSet<>(fieldProcessors.size());
            while (parser.currentToken() == JsonToken.FIELD_NAME) {
                final String fieldName = parser.currentName();
                final ValueProcessor knownProcessor = fieldProcessors.get(fieldName);
                if (knownProcessor == null) {
                    if (!allowUnknownFields()) {
                        // todo json exception
                        throw new IllegalStateException(String.format("Unexpected field '%s'", fieldName));
                    }
                    parser.nextToken();
                    parser.skipChildren();
                } else if (visited.add(fieldName)) {
                    // First time seeing field
                    parser.nextToken();
                    knownProcessor.processCurrentValue(parser);
                } else if (repeatedFieldBehavior() == RepeatedFieldBehavior.USE_FIRST) {
                    parser.nextToken();
                    parser.skipChildren();
                } else {
                    throw new IllegalStateException("todo");
                }
                parser.nextToken();
            }
            assertCurrentToken(parser, JsonToken.END_OBJECT);
            for (Entry<String, ValueProcessor> e : fieldProcessors.entrySet()) {
                if (!visited.contains(e.getKey())) {
                    e.getValue().processMissing(parser);
                }
            }
        }
    }
}
