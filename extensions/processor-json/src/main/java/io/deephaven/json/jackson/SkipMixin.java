/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.SkipOptions;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class SkipMixin extends Mixin<SkipOptions> {

    public SkipMixin(SkipOptions options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int outputCount() {
        return 0;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.empty();
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return Stream.empty();
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new Impl();
    }

    class Impl implements ValueProcessor {
        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_OBJECT:
                    if (!options.allowObject()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    parser.skipChildren();
                    break;
                case START_ARRAY:
                    if (!options.allowArray()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    parser.skipChildren();
                    break;
                case VALUE_STRING:
                    if (!options.allowString()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                case VALUE_NUMBER_INT:
                    if (!options.allowNumberInt()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                case VALUE_NUMBER_FLOAT:
                    if (!options.allowNumberFloat()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                case VALUE_TRUE:
                case VALUE_FALSE:
                    if (!options.allowBoolean()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                case VALUE_NULL:
                    if (!options.allowNull()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                default:
                    throw Helpers.mismatch(parser, void.class);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
                throw Helpers.mismatchMissing(parser, void.class);
            }
        }
    }
}
