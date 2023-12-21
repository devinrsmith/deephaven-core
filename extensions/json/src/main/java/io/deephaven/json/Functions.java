/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

public final class Functions {

    public interface ToInt {

        int applyAsInt(JsonParser parser) throws IOException;

        enum Plain implements ToInt {
            INT_VALUE;

            @Override
            public int applyAsInt(JsonParser parser) throws IOException {
                return parser.getIntValue();
            }
        }
    }

    public interface ToLong {

        long applyAsLong(JsonParser parser) throws IOException;

        enum Plain implements ToLong {
            LONG_VALUE;

            @Override
            public long applyAsLong(JsonParser parser) throws IOException {
                return parser.getLongValue();
            }
        }
    }

    public interface ToObject<T> {

        T apply(JsonParser parser) throws IOException;

        enum Plain implements ToObject<String> {
            STRING_VALUE;

            @Override
            public String apply(JsonParser parser) throws IOException {
                return parser.getText();
            }
        }
    }
}
