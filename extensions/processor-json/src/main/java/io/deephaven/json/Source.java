/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.Objects;

public interface Source {

    static Source of(String content) {
        Objects.requireNonNull(content);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(content);
            }
        };
    }

    static Source of(ByteBuffer buffer) {
        Objects.requireNonNull(buffer);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(buffer);
            }
        };
    }

    static Source of(CharBuffer buffer) {
        Objects.requireNonNull(buffer);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(buffer);
            }
        };
    }

    static Source of(File file) {
        Objects.requireNonNull(file);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(file);
            }
        };
    }

    static Source of(Path path) {
        Objects.requireNonNull(path);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(path);
            }
        };
    }

    static Source of(InputStream inputStream) {
        Objects.requireNonNull(inputStream);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(inputStream);
            }
        };
    }

    static Source of(URL url) {
        Objects.requireNonNull(url);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(url);
            }
        };
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        // todo: charSource?
        T visit(String content);

        T visit(ByteBuffer buffer);

        T visit(CharBuffer buffer);

        T visit(File file);

        T visit(Path path);

        T visit(InputStream inputStream);

        T visit(URL url);
    }
}
