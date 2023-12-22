/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.Objects;

abstract class ValueProcessorBase implements ValueProcessor {
    protected final String context;
    private final boolean allowNull;
    private final boolean allowMissing;

    ValueProcessorBase(String context, boolean allowNull, boolean allowMissing) {
        this.context = Objects.requireNonNull(context);
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
                    throw new IllegalStateException(
                            String.format("[%s]: Unexpected null value, allowNull=false", context));
                }
                handleNull(parser);
                break;
            default:
                throw unexpected(token);
        }
    }

    @Override
    public final void processMissing(JsonParser parser) throws IOException {
        if (!allowMissing) {
            throw new IllegalStateException(
                    String.format("[%s]: Unexpected missing value, allowMissing=false", context));
        }
        handleMissing(parser);
    }

    protected abstract void handleNull(JsonParser parser) throws IOException;

    protected abstract void handleMissing(JsonParser parser) throws IOException;

    protected void handleValueObject(JsonParser parser) throws IOException {
        throw unexpected(JsonToken.START_OBJECT);
    }

    protected void handleValueArray(JsonParser parser) throws IOException {
        throw unexpected(JsonToken.START_ARRAY);
    }

    protected void handleValueString(JsonParser parser) throws IOException {
        throw unexpected(JsonToken.VALUE_STRING);
    }

    protected void handleValueNumberInt(JsonParser parser) throws IOException {
        throw unexpected(JsonToken.VALUE_NUMBER_INT);
    }

    protected void handleValueNumberFloat(JsonParser parser) throws IOException {
        throw unexpected(JsonToken.VALUE_NUMBER_FLOAT);
    }

    protected void handleValueTrue(JsonParser parser) {
        throw unexpected(JsonToken.VALUE_TRUE);
    }

    protected void handleValueFalse(JsonParser parser) {
        throw unexpected(JsonToken.VALUE_FALSE);
    }

    private IllegalStateException unexpected(JsonToken token) {
        return new IllegalStateException(String.format("[%s]: Unexpected token %s", context, token));
    }
}
