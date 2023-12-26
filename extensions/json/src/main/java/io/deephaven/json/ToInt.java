/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

interface ToInt extends Function {

    int parseValue(JsonParser parser) throws IOException;

    int parseMissing(JsonParser parser) throws IOException;
}
