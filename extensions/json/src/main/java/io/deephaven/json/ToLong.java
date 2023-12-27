/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

interface ToLong {

    long parseValue(JsonParser parser) throws IOException;

    long parseMissing(JsonParser parser) throws IOException;
}
