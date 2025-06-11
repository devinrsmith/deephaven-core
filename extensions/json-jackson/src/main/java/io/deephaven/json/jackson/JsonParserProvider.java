//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;

import java.util.function.Supplier;

public interface JsonParserProvider extends Supplier<JsonParser> {

}
