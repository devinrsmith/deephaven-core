/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

abstract class ValueProcessorBase implements ValueProcessor {
    protected final String contextPrefix;
    private final boolean allowNull;
    private final boolean allowMissing;

    ValueProcessorBase(String contextPrefix, boolean allowNull, boolean allowMissing) {
        this.contextPrefix = contextPrefix;
        this.allowNull = allowNull;
        this.allowMissing = allowMissing;
    }

    @Override
    public final void processCurrentValue(JsonParser parser) throws IOException {
        final JsonToken token = parser.currentToken();
        switch (token) {
            case START_OBJECT:
                handleValueObject(parser);
                break;
            case START_ARRAY:
                handleValueArray(parser);
                break;
            case VALUE_STRING:
                handleValueString(parser);
                break;
            case VALUE_NUMBER_INT:
                handleValueNumberInt(parser);
                break;
            case VALUE_NUMBER_FLOAT:
                handleValueNumberFloat(parser);
                break;
            case VALUE_TRUE:
                handleValueTrue(parser);
                break;
            case VALUE_FALSE:
                handleValueFalse(parser);
                break;
            case VALUE_NULL:
                if (!allowNull) {
                    throw new IllegalStateException(String.format("%sUnexpected null, allowNull=false", contextPrefix));
                }
                handleNull();
                break;
            default:
                throw new IllegalStateException(String.format("%sUnexpected token %s, expected VALUE_NUMBER_INT or VALUE_NULL", contextPrefix, token));
        }
    }

    protected abstract void handleNull();

    protected abstract void handleMissing();

    protected void handleValueObject(JsonParser parser) throws IOException {
        throw new IllegalStateException(String.format("%sUnexpected type VALUE_OBJECT", contextPrefix));
    }

    protected void handleValueArray(JsonParser parser) throws IOException {
        throw new IllegalStateException(String.format("%sUnexpected type VALUE_ARRAY", contextPrefix));
    }

    protected void handleValueString(JsonParser parser) throws IOException {
        throw new IllegalStateException(String.format("%sUnexpected type VALUE_STRING", contextPrefix));
    }

    protected void handleValueNumberInt(JsonParser parser) throws IOException {
        throw new IllegalStateException(String.format("%sUnexpected type VALUE_NUMBER_INT", contextPrefix));
    }

    protected void handleValueNumberFloat(JsonParser parser) throws IOException {
        throw new IllegalStateException(String.format("%sUnexpected type VALUE_NUMBER_FLOAT", contextPrefix));
    }

    protected void handleValueTrue(JsonParser parser) throws IOException {
        throw new IllegalStateException(String.format("%sUnexpected type VALUE_TRUE", contextPrefix));
    }

    protected void handleValueFalse(JsonParser parser) throws IOException {
        throw new IllegalStateException(String.format("%sUnexpected type VALUE_FALSE", contextPrefix));
    }

    @Override
    public final void processMissing() {
        if (!allowMissing) {
            throw new IllegalStateException(String.format("%sUnexpected missing, allowMissing=false", contextPrefix));
        }
        handleMissing();
    }
}
