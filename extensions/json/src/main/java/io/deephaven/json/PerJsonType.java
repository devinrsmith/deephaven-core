/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;

public abstract class PerJsonType<T> {

    public abstract T onObject();

    public abstract T onArray();

    public abstract T onNull();

    public abstract T onBoolean();

    public abstract T onString();

    public abstract T onNumberInt();

    public abstract T onNumberFloat();

    final T onToken(JsonToken jsonToken) {
        switch (jsonToken) {
            case START_OBJECT:
                return onObject();
            case START_ARRAY:
                return onArray();
            case VALUE_STRING:
                return onString();
            case VALUE_NUMBER_INT:
                return onNumberInt();
            case VALUE_NUMBER_FLOAT:
                return onNumberFloat();
            case VALUE_TRUE:
            case VALUE_FALSE:
                return onBoolean();
            case VALUE_NULL:
                return onNull();
        }
        return null;
    }

    public interface Builder<T, B extends Builder<T, B>> {
        B onObject(T onObject);

        B onArray(T onArray);

        B onString(T onString);

        B onNumberInt(T onNumberInt);

        B onNumberFloat(T onNumberFloat);

        B onBoolean(T onBoolean);

        B onNull(T onNull);

        T build();
    }
}
