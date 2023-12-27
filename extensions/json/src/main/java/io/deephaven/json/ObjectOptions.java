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

    public static Builder builder() {
        return ImmutableObjectOptions.builder();
    }

    public static ObjectOptions of(Map<String, ValueOptions> fields) {
        return builder().putAllFieldProcessors(fields).build();
    }

    public abstract Map<String, ValueOptions> fieldProcessors();

    /**
     * ...
     *
     * <p>
     * If the caller wants to throw an error on unknown fields, but knows there are fields they want to skip, the can be
     * e todo To be more selective, individual fields can be added with {@link SkipOptions} ... todoeueou
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

    public final SkipOptions skip() {
        // todo: this doesn't make sense on this object
        return SkipOptions.builder()
                .allowObject(true)
                .allowNull(allowNull())
                .allowMissing(allowMissing())
                .build();
    }

    public interface Builder extends ValueOptions.Builder<ObjectOptions, Builder> {
        Builder allowUnknownFields(boolean allowUnknownFields);

        Builder repeatedFieldBehavior(RepeatedFieldBehavior repeatedFieldBehavior);

        Builder putFieldProcessors(String key, ValueOptions value);

        Builder putFieldProcessors(Map.Entry<String, ? extends ValueOptions> entry);

        Builder putAllFieldProcessors(Map<String, ? extends ValueOptions> entries);
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
        return new ObjectValueFieldProcessor(processors);
    }


    private class ObjectValueFieldProcessor implements ValueProcessor {
        private final Map<String, ValueProcessor> fieldProcessors;

        ObjectValueFieldProcessor(Map<String, ValueProcessor> fieldProcessors) {
            this.fieldProcessors = Objects.requireNonNull(fieldProcessors);
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_OBJECT:
                    parseObject(parser);
                    return;
                case VALUE_NULL:
                    parseNull(parser);
                    return;
                default:
                    throw Helpers.mismatch(parser, Object.class);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            parseMissing(parser);
        }

        private void parseObject(JsonParser parser) throws IOException {
            // Note: we could try to build a stricter implementation that doesn't use Set; if the user can guarantee
            // that none of the fields will be missing and there won't be any repeated fields, we could use a simple
            // counter to ensure all field processors were invoked.
            final Set<String> visited = new HashSet<>(fieldProcessors.size());
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
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
            }
            assertCurrentToken(parser, JsonToken.END_OBJECT);
            for (Entry<String, ValueProcessor> e : fieldProcessors.entrySet()) {
                if (!visited.contains(e.getKey())) {
                    e.getValue().processMissing(parser);
                }
            }
        }

        private void parseNull(JsonParser parser) throws IOException {
            if (!allowNull()) {
                throw Helpers.mismatch(parser, Object.class);
            }
            // Note: we are treating a null object the same as an empty object
            // null ~= {}
            for (ValueProcessor value : fieldProcessors.values()) {
                value.processMissing(parser);
            }
        }

        private void parseMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Helpers.mismatchMissing(parser, Object.class);
            }
            // Note: we are treating a missing object the same as an empty object
            // # field_name: <not-present>
            // field_name: {}
            for (ValueProcessor value : fieldProcessors.values()) {
                value.processMissing(parser);
            }
        }
    }
}
