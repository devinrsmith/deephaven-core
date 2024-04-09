//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

final class JacksonSource {

    public static JsonParser of(JsonFactory factory, String content) throws IOException {
        return factory.createParser(content);
    }

    public static JsonParser of(JsonFactory factory, File file) throws IOException {
        return factory.createParser(file);
    }

    // todo: suggest jackson build this in
    public static JsonParser of(JsonFactory factory, Path path) throws IOException {
        if (FileSystems.getDefault() == path.getFileSystem()) {
            return of(factory, path.toFile());
        }
        if (!factory.isEnabled(StreamReadFeature.AUTO_CLOSE_SOURCE)) {
            throw new RuntimeException(String.format("Unable to create Path-based parser when '%s' is not enabled",
                    StreamReadFeature.AUTO_CLOSE_SOURCE));
        }
        // jackson buffers internally
        return factory.createParser(Files.newInputStream(path));
    }

    public static JsonParser of(JsonFactory factory, InputStream inputStream) throws IOException {
        return factory.createParser(inputStream);
    }

    public static JsonParser of(JsonFactory factory, URL url) throws IOException {
        return factory.createParser(url);
    }

    // todo: suggest jackson build this in
    public static JsonParser of(JsonFactory factory, ByteBuffer buffer) throws IOException {
        if (buffer.hasArray()) {
            return factory.createParser(buffer.array(), buffer.position(), buffer.remaining());
        }
        return of(factory, ByteBufferInputStream.of(buffer));
    }

    // todo: suggest jackson build this in
    public static JsonParser of(JsonFactory factory, CharBuffer buffer) throws IOException {
        if (buffer.hasArray()) {
            return factory.createParser(buffer.array(), buffer.position(), buffer.remaining());
        }
        // todo: we could build CharBufferReader. Surprised it's not build into JDK.
        throw new RuntimeException("todo");
    }
}
