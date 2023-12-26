/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

interface ToFloat extends Function {

    float parseValue(JsonParser parser) throws IOException;

    float parseMissing(JsonParser parser) throws IOException;
}
