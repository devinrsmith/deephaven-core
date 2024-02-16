/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.TupleOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.deephaven.json.jackson.Helpers.assertCurrentToken;

final class TupleMixin extends Mixin {
    private final TupleOptions options;

    public TupleMixin(TupleOptions options, JacksonConfiguration factory) {
        super(factory);
        this.options = Objects.requireNonNull(options);
    }

    @Override
    public int outputCount() {
        return options.values().stream().map(this::mixin).mapToInt(Mixin::outputCount).sum();
    }

    @Override
    public Stream<List<String>> paths() {
        // todo, give user naming option?
        final List<Stream<List<String>>> prefixed = new ArrayList<>();
        int i = 0;
        for (ValueOptions value : options.values()) {
            final int ix = i;
            prefixed.add(mixin(value).paths().map(x -> prefixWith("tuple_" + ix, x)));
            ++i;
        }
        return prefixed.stream().flatMap(Function.identity());
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return options.values().stream().map(this::mixin).flatMap(Mixin::outputTypes);
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final List<ValueProcessor> processors = new ArrayList<>(options.values().size());
        int ix = 0;
        int processorIx = 0;
        for (ValueOptions value : options.values()) {
            final Mixin mixin = mixin(value);
            final int numTypes = mixin.numColumns();
            final ValueProcessor processor =
                    mixin.processor(context + "[" + processorIx + "]", out.subList(ix, ix + numTypes));
            processors.add(processor);
            ix += numTypes;
            ++processorIx;
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
            if (!options.allowNull()) {
                throw Helpers.mismatch(parser, Object.class);
            }
            // Note: we are treating a null tuple the same as a tuple of null objects
            // null ~= [null, ..., null]
            for (ValueProcessor value : values) {
                value.processCurrentValue(parser);
            }
        }

        private void parseMissing(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
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
