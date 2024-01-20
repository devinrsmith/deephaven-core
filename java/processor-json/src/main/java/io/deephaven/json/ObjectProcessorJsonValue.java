/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

abstract class ObjectProcessorJsonValue<T> implements ObjectProcessor<T> {

    private final ValueOptions opts;
    private final JsonFactory factory;

    ObjectProcessorJsonValue(ValueOptions opts, JsonFactory factory) {
        this.opts = Objects.requireNonNull(opts);
        this.factory = Objects.requireNonNull(factory);
    }

    protected abstract JsonParser createParser(JsonFactory factory, T in) throws IOException;

    @Override
    public final int size() {
        return opts.outputCount();
    }

    @Override
    public final List<Type<?>> outputTypes() {
        return opts.outputTypes().collect(Collectors.toList());
    }

    @Override
    public final void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        try {
            processAllImpl(in, out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void processAllImpl(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) throws IOException {
        final ValueProcessor valueProcessor = opts.processor("<root>", out);
        for (int i = 0; i < in.size(); ++i) {
            try (final JsonParser parser = createParser(factory, in.get(i))) {
                ValueProcessor.processFullJson(valueProcessor, parser);
            }
        }
    }

    static final class StringIn extends ObjectProcessorJsonValue<String> {
        StringIn(ValueOptions opts, JsonFactory factory) {
            super(opts, factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, String in) throws IOException {
            return factory.createParser(in);
        }
    }

    static final class BytesIn extends ObjectProcessorJsonValue<byte[]> {
        BytesIn(ValueOptions opts, JsonFactory factory) {
            super(opts, factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, byte[] in) throws IOException {
            return factory.createParser(in);
        }
    }

    static final class CharsIn extends ObjectProcessorJsonValue<char[]> {
        CharsIn(ValueOptions opts, JsonFactory factory) {
            super(opts, factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, char[] in) throws IOException {
            return factory.createParser(in);
        }
    }

    static final class FileIn extends ObjectProcessorJsonValue<File> {
        FileIn(ValueOptions opts, JsonFactory factory) {
            super(opts, factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, File in) throws IOException {
            return factory.createParser(in);
        }
    }

    static final class URLIn extends ObjectProcessorJsonValue<URL> {
        URLIn(ValueOptions opts, JsonFactory factory) {
            super(opts, factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, URL in) throws IOException {
            return factory.createParser(in);
        }
    }
}
