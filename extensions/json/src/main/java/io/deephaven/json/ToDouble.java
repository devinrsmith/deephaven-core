/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

interface ToDouble {

    double parseValue(JsonParser parser) throws IOException;

    double parseMissing(JsonParser parser) throws IOException;
}
