/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import java.util.Map;

public class HomeAssistant {

    public static ObjectOptions attributes() {
        return ObjectOptions.of(Map.of(
                "unit_standard_measurement", StringOptions.standard(),
                "device_class", StringOptions.standard(),
                "icon", StringOptions.standard(),
                "friendly_name", StringOptions.standard()));
    }

    public static ObjectOptions context() {
        return ObjectOptions.of(Map.of(
                "id", StringOptions.standard(),
                "parent_id", StringOptions.standard(),
                "user_id", StringOptions.standard()));
    }

    public static ObjectOptions full() {
        return ObjectOptions.of(Map.of(
                "entity_id", StringOptions.standard(),
                "state", StringOptions.standard(),
                "attributes", attributes(),
                "last_changed", InstantOptions.standard(),
                "last_updated", InstantOptions.standard(),
                "context", context()));
    }
}
