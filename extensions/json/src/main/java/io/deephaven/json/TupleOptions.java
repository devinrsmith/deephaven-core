/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static io.deephaven.json.Helpers.assertCurrentToken;

@Immutable
@BuildableStyle
public abstract class TupleOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableTupleOptions.builder();
    }

    public static TupleOptions of(ValueOptions... values) {
        return builder().addValues(values).build();
    }

    public static TupleOptions of(Iterable<? extends ValueOptions> values) {
        return builder().addAllValues(values).build();
    }

    public abstract List<ValueOptions> values();

    @Override
    public final boolean allowNull() {
        return values().stream().allMatch(ValueOptions::allowNull);
    }

    @Override
    public final boolean allowMissing() {
        return values().stream().allMatch(ValueOptions::allowMissing);
    }

    // Note: Builder does not extend ValueOptions.Builder b/c allowNull / allowMissing is implicitly set

    public interface Builder {

        Builder addValues(ValueOptions element);

        Builder addValues(ValueOptions... elements);

        Builder addAllValues(Iterable<? extends ValueOptions> elements);

        TupleOptions build();
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return values().stream().flatMap(ValueOptions::outputTypes);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final List<ValueProcessor> processors = new ArrayList<>(values().size());
        int ix = 0;
        for (ValueOptions value : values()) {
            final int numTypes = value.numColumns();
            final ValueProcessor processor = value.processor(context + "[" + ix + "]", out.subList(ix, ix + numTypes));
            processors.add(processor);
            ix += numTypes;
        }
        if (ix != out.size()) {
            throw new IllegalStateException();
        }
        return new TupleProcessor(processors);
    }

    private class TupleProcessor implements ValueProcessor {
        private final List<ValueProcessor> values;

        public TupleProcessor(List<ValueProcessor> values) {
            this.values = Objects.requireNonNull(values);
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_ARRAY:
                    parseTuple(parser);
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

        private void parseTuple(JsonParser parser) throws IOException {
            for (ValueProcessor value : values) {
                parser.nextToken();
                value.processCurrentValue(parser);
            }
            parser.nextToken();
            assertCurrentToken(parser, JsonToken.END_ARRAY);
        }

        private void parseNull(JsonParser parser) throws IOException {
            if (!allowNull()) {
                throw Helpers.mismatch(parser, Object.class);
            }
            // Note: we are treating a null tuple the same as a tuple of null objects
            // null ~= [null, ..., null]
            for (ValueProcessor value : values) {
                value.processCurrentValue(parser);
            }
        }

        private void parseMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Helpers.mismatchMissing(parser, Object.class);
            }
            // Note: we are treating a missing tuple the same as a tuple of missing objects (which, is technically
            // impossible w/ native json, but it's the semantics we are exposing).
            // <missing> ~= [<missing>, ..., <missing>]
            for (ValueProcessor value : values) {
                value.processMissing(parser);
            }
        }
    }
}
