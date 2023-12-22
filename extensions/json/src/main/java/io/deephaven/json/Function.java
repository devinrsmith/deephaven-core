/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.util.QueryConstants;

import java.io.IOException;

public interface Function {

    interface ToInt extends Function {

        int applyAsInt(JsonParser parser) throws IOException;

        enum Plain implements ToInt {
            INT_VALUE;

            @Override
            public int applyAsInt(JsonParser parser) throws IOException {
                return parser.getIntValue();
            }
        }
    }

    interface ToLong extends Function {

        long applyAsLong(JsonParser parser) throws IOException;

        enum Parser implements ToLong {
            LONG_VALUE;

            @Override
            public long applyAsLong(JsonParser parser) throws IOException {
                return parser.getLongValue();
            }
        }

        enum DhNull implements ToLong {
            /**
             * Equivalent to the constant {@link QueryConstants#NULL_LONG}.
             */
            DH_NULL;

            @Override
            public long applyAsLong(JsonParser parser) {
                return QueryConstants.NULL_LONG;
            }
        }
    }

    interface ToDouble extends Function {

        double applyAsDouble(JsonParser parser) throws IOException;

        enum Parser implements ToDouble {
            /**
             * Equivalent to {@link JsonParser#getDoubleValue()}.
             */
            DOUBLE_VALUE;

            @Override
            public double applyAsDouble(JsonParser parser) throws IOException {
                return parser.getDoubleValue();
            }
        }

        enum DhNull implements ToDouble {
            /**
             * Equivalent to the constant {@link QueryConstants#NULL_DOUBLE}.
             */
            DH_NULL;

            @Override
            public double applyAsDouble(JsonParser parser) {
                return QueryConstants.NULL_DOUBLE;
            }
        }
    }

    interface ToObject<T> extends Function {

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
