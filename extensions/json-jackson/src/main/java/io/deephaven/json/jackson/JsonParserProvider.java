//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.function.Supplier;

public interface JsonParserProvider extends Supplier<JsonParser> {

    static JsonParserProvider of(final String source) {
        return of(JacksonConfiguration.defaultFactory(), source);
    }

    static JsonParserProvider of(final File source) {
        return of(JacksonConfiguration.defaultFactory(), source);
    }

    static JsonParserProvider of(final Path source) {
        return of(JacksonConfiguration.defaultFactory(), source);
    }

    static JsonParserProvider of(final InputStream source) {
        return of(JacksonConfiguration.defaultFactory(), source);
    }

    static JsonParserProvider of(final URL source) {
        return of(JacksonConfiguration.defaultFactory(), source);
    }

    static JsonParserProvider of(final ByteBuffer source) {
        return of(JacksonConfiguration.defaultFactory(), source);
    }

    static JsonParserProvider of(final CharBuffer source) {
        return of(JacksonConfiguration.defaultFactory(), source);
    }

    static JsonParserProvider of(final byte[] array, final int offset, final int len) {
        return of(JacksonConfiguration.defaultFactory(), array, offset, len);
    }

    static JsonParserProvider of(final char[] array, final int offset, final int len) {
        return of(JacksonConfiguration.defaultFactory(), array, offset, len);
    }

    static JsonParserProvider of(final JsonFactory factory, final String source) {
        return () -> JacksonSource.of(factory, source);
    }

    static JsonParserProvider of(final JsonFactory factory, final File source) {
        return () -> JacksonSource.of(factory, source);
    }

    static JsonParserProvider of(final JsonFactory factory, final Path source) {
        return () -> JacksonSource.of(factory, source);
    }

    static JsonParserProvider of(final JsonFactory factory, final InputStream source) {
        return () -> JacksonSource.of(factory, source);
    }

    static JsonParserProvider of(final JsonFactory factory, final URL source) {
        return () -> JacksonSource.of(factory, source);
    }

    static JsonParserProvider of(final JsonFactory factory, final ByteBuffer source) {
        return () -> JacksonSource.of(factory, source);
    }

    static JsonParserProvider of(final JsonFactory factory, final CharBuffer source) {
        return () -> JacksonSource.of(factory, source);
    }

    static JsonParserProvider of(final JsonFactory factory, final byte[] array, final int offset, final int len) {
        return () -> JacksonSource.of(factory, array, offset, len);
    }

    static JsonParserProvider of(final JsonFactory factory, final char[] array, final int offset, final int len) {
        return () -> JacksonSource.of(factory, array, offset, len);
    }

    JsonParser create() throws IOException;

    @Override
    default JsonParser get() {
        try {
            return create();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
