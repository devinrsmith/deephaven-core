/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import java.util.EnumSet;

public enum JsonValueTypes {
    OBJECT, ARRAY, STRING, NUMBER_INT, NUMBER_FLOAT, BOOL, NULL;

    static final EnumSet<JsonValueTypes> ALL = EnumSet.allOf(JsonValueTypes.class);
    static final EnumSet<JsonValueTypes> NUMBER_LIKE = EnumSet.of(STRING, NUMBER_INT, NUMBER_FLOAT, NULL);
    static final EnumSet<JsonValueTypes> STRING_LIKE = EnumSet.of(STRING, NUMBER_INT, NUMBER_FLOAT, BOOL, NULL);
    static final EnumSet<JsonValueTypes> STRING_OR_NULL = STRING.orNull();
    static final EnumSet<JsonValueTypes> OBJECT_OR_NULL = OBJECT.orNull();
    static final EnumSet<JsonValueTypes> ARRAY_OR_NULL = ARRAY.orNull();
    static final EnumSet<JsonValueTypes> NUMBER_INT_OR_NULL = NUMBER_INT.orNull();

    EnumSet<JsonValueTypes> orNull() {
        return EnumSet.of(this, NULL);
    }
}
