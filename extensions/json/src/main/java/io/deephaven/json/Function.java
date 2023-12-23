/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.util.QueryConstants;

import java.io.IOException;

public interface Function {

    interface ToInt extends Function {

        int parseValue(JsonParser parser) throws IOException;

        enum Plain implements ToInt {
            INT_VALUE;

            @Override
            public int parseValue(JsonParser parser) throws IOException {
                return parser.getIntValue();
            }
        }
    }

    interface ToDouble extends Function {

        double parseValue(JsonParser parser) throws IOException;

        enum Parser implements ToDouble {
            /**
             * Equivalent to {@link JsonParser#getDoubleValue()}.
             */
            DOUBLE_VALUE;

            @Override
            public double parseValue(JsonParser parser) throws IOException {
                return parser.getDoubleValue();
            }
        }

        enum DhNull implements ToDouble {
            /**
             * Equivalent to the constant {@link QueryConstants#NULL_DOUBLE}.
             */
            DH_NULL;

            @Override
            public double parseValue(JsonParser parser) {
                return QueryConstants.NULL_DOUBLE;
            }
        }
    }

}
