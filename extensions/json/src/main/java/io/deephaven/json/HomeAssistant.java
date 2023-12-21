/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

public class HomeAssistant {

    public static ObjectOptions attributes() {
        return ObjectOptions.builder()
                .putFieldProcessors("unit_of_measurement", StringOptions.of())
                .putFieldProcessors("device_class", StringOptions.of())
                .putFieldProcessors("icon", StringOptions.of())
                .putFieldProcessors("friendly_name", StringOptions.of())
                .build();
    }

    public static ObjectOptions context() {
        return ObjectOptions.builder()
                .putFieldProcessors("id", StringOptions.of())
                .putFieldProcessors("parent_id", StringOptions.of())
                .putFieldProcessors("user_id", StringOptions.of())
                .build();
    }

    public static ObjectOptions full() {
        return ObjectOptions.builder()
                .putFieldProcessors("entity_id", StringOptions.of())
                .putFieldProcessors("state", StringOptions.of())
                .putFieldProcessors("attributes", attributes())
                .putFieldProcessors("last_changed", DateTimeOptions.of())
                .putFieldProcessors("last_updated", DateTimeOptions.of())
                .putFieldProcessors("context", context())
                .build();
    }
}
