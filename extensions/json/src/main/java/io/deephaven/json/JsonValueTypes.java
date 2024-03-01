/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import java.util.EnumSet;
import java.util.Set;

public enum JsonValueTypes {
    OBJECT, ARRAY, STRING, INT, DECIMAL, BOOL, NULL;

    static final EnumSet<JsonValueTypes> ALL = EnumSet.allOf(JsonValueTypes.class);
    static final EnumSet<JsonValueTypes> NUMBER_LIKE = EnumSet.of(STRING, INT, DECIMAL, NULL);
    static final EnumSet<JsonValueTypes> INT_LIKE = EnumSet.of(STRING, INT, NULL);
    static final EnumSet<JsonValueTypes> STRING_LIKE = EnumSet.of(STRING, INT, DECIMAL, BOOL, NULL);
    static final EnumSet<JsonValueTypes> STRING_OR_NULL = STRING.orNull();
    static final EnumSet<JsonValueTypes> OBJECT_OR_NULL = OBJECT.orNull();
    static final EnumSet<JsonValueTypes> ARRAY_OR_NULL = ARRAY.orNull();
    static final EnumSet<JsonValueTypes> INT_OR_NULL = INT.orNull();
    static final EnumSet<JsonValueTypes> NUMBER = EnumSet.of(INT, DECIMAL);
    static final EnumSet<JsonValueTypes> NUMBER_OR_NULL = EnumSet.of(INT, DECIMAL, NULL);

    static void checkInvariants(Set<JsonValueTypes> set) {
        if (set.isEmpty()) {
            throw new IllegalArgumentException("set is empty");
        }
        if (set.size() == 1 && set.contains(JsonValueTypes.NULL)) {
            // does it ever make sense to _only_ accept null?
        }
        if (set.contains(JsonValueTypes.DECIMAL) && !set.contains(JsonValueTypes.INT)) {
            throw new IllegalArgumentException("Accepting NUMBER_FLOAT but not NUMBER_INT");
        }
    }

    EnumSet<JsonValueTypes> asSet() {
        return EnumSet.of(this);
    }

    EnumSet<JsonValueTypes> orNull() {
        return EnumSet.of(this, NULL);
    }
}
