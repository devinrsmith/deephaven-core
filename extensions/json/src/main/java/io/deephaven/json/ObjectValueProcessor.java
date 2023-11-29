/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static io.deephaven.json.Helpers.assertCurrentToken;

final class ObjectValueProcessor extends ValueProcessorBase {
    private final Map<String, ValueProcessor> fieldProcessors;
    private final ValueProcessor onUnknownFieldProcessor;
    private final ValueProcessor onMultipleFieldProcessor;

    ObjectValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, Map<String, ValueProcessor> fieldProcessors, ValueProcessor onUnknownFieldProcessor, ValueProcessor onMultipleFieldProcessor) {
        super(contextPrefix, allowNull, allowMissing);
        this.fieldProcessors = Map.copyOf(fieldProcessors);
        this.onUnknownFieldProcessor = onUnknownFieldProcessor;
        this.onMultipleFieldProcessor = onMultipleFieldProcessor;
    }

    @Override
    protected void handleValueObject(JsonParser parser) throws IOException {
        final Set<String> visited = new HashSet<>(fieldProcessors.size());
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            final String fieldName = parser.currentName();
            final ValueProcessor processor;
            {
                final ValueProcessor knownProcessor = fieldProcessors.get(fieldName);
                if (knownProcessor != null) {
                    if (visited.add(fieldName)) {
                        // first time seeing this field name, process normally.
                        processor = knownProcessor;
                    } else if (onMultipleFieldProcessor != null) {
                        // todo: we could consider adding ValueProcessor#processMultiple(...)
                        processor = onMultipleFieldProcessor;
                    } else {
                        throw new IllegalStateException(String.format("Repeated field '%s'", fieldName));
                    }
                } else if (onUnknownFieldProcessor != null) {
                    processor = onUnknownFieldProcessor;
                } else {
                    throw new IllegalStateException(String.format("Unexpected field '%s'", fieldName));
                }
            }
            parser.nextToken();
            processor.processCurrentValue(parser);
        }
        assertCurrentToken(parser, JsonToken.END_OBJECT);
        for (Entry<String, ValueProcessor> e : fieldProcessors.entrySet()) {
            if (!visited.contains(e.getKey())) {
                e.getValue().processMissing();
            }
        }
    }

    @Override
    protected void handleNull() {
        // Note: we are treating a null object the same as an empty object
        // field_name: null
        // field_name: {}
        for (ValueProcessor value : fieldProcessors.values()) {
            value.processMissing();
        }
    }

    @Override
    protected void handleMissing() {
        // Note: we are treating a missing object the same as an empty object
        // # field_name: ...
        // field_name: {}
        for (ValueProcessor value : fieldProcessors.values()) {
            value.processMissing();
        }
    }
}
