/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.ObjectOptions.RepeatedFieldBehavior;
import io.deephaven.json.ValueOptions;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static io.deephaven.json.jackson.Parsing.assertCurrentToken;

final class ObjectMixin extends Mixin<ObjectOptions> {

    public ObjectMixin(ObjectOptions options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return options.fields().values().stream().map(this::mixin).flatMap(Mixin::outputTypes);
    }

    @Override
    public int numColumns() {
        return options.fields().values().stream().map(this::mixin).mapToInt(Mixin::numColumns).sum();
    }

    @Override
    public Stream<List<String>> paths() {
        return prefixWithKeys(options.fields());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final Map<String, ValueProcessor> processors = new LinkedHashMap<>(options.fields().size());
        int ix = 0;
        for (Entry<String, ValueOptions> e : options.fields().entrySet()) {
            final String fieldName = e.getKey();
            final Mixin<?> opts = mixin(e.getValue());
            final int numTypes = opts.numColumns();
            final ValueProcessor fieldProcessor =
                    opts.processor(context + "/" + fieldName, out.subList(ix, ix + numTypes));
            processors.put(fieldName, fieldProcessor);
            ix += numTypes;
        }
        if (ix != out.size()) {
            throw new IllegalStateException();
        }
        return processorImpl(processors);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final Map<String, RepeaterProcessor> processors = new LinkedHashMap<>(this.options.fields().size());
        int ix = 0;
        for (Entry<String, ValueOptions> e : this.options.fields().entrySet()) {
            final String fieldName = e.getKey();
            final Mixin<?> opts = mixin(e.getValue());
            final int numTypes = opts.numColumns();
            final RepeaterProcessor fieldProcessor =
                    opts.repeaterProcessor(allowMissing, allowNull, out.subList(ix, ix + numTypes));
            processors.put(fieldName, fieldProcessor);
            ix += numTypes;
        }
        if (ix != out.size()) {
            throw new IllegalStateException();
        }
        return new ObjectValueRepeaterProcessor(processors);
    }

    ObjectValueFieldProcessor processorImpl(Map<String, ValueProcessor> processors) {
        return new ObjectValueFieldProcessor(processors);
    }

    final class ObjectValueFieldProcessor implements ValueProcessor {
        private final Map<String, ValueProcessor> fieldProcessors;

        ObjectValueFieldProcessor(Map<String, ValueProcessor> fieldProcessors) {
            this.fieldProcessors = Objects.requireNonNull(fieldProcessors);
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            // see com.fasterxml.jackson.databind.JsonDeserializer.deserialize(com.fasterxml.jackson.core.JsonParser,
            // com.fasterxml.jackson.databind.DeserializationContext)
            // for notes on FIELD_NAME
            switch (parser.currentToken()) {
                case START_OBJECT:
                    if (parser.nextToken() == JsonToken.END_OBJECT) {
                        processEmptyObject(parser);
                        return;
                    }
                    if (!parser.hasToken(JsonToken.FIELD_NAME)) {
                        throw new IllegalStateException();
                    }
                    // fall-through
                case FIELD_NAME:
                    processObjectFields(parser);
                    return;
                case VALUE_NULL:
                    processNullObject(parser);
                    return;
                default:
                    throw Parsing.mismatch(parser, Object.class);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
                throw Parsing.mismatchMissing(parser, Object.class);
            }
            for (ValueProcessor value : fieldProcessors.values()) {
                value.processMissing(parser);
            }
        }

        private void processNullObject(JsonParser parser) throws IOException {
            if (!options.allowNull()) {
                throw Parsing.mismatch(parser, Object.class);
            }
            for (ValueProcessor value : fieldProcessors.values()) {
                value.processCurrentValue(parser);
            }
        }

        private void processEmptyObject(JsonParser parser) throws IOException {
            // This logic should be equivalent to processObjectFields, but where we know there are no fields
            for (ValueProcessor value : fieldProcessors.values()) {
                value.processMissing(parser);
            }
        }

        private void processObjectFields(JsonParser parser) throws IOException {
            // Note: we could try to build a stricter implementation that doesn't use Set; if the user can guarantee
            // that none of the fields will be missing and there won't be any repeated fields, we could use a simple
            // counter to ensure all field processors were invoked.
            final Set<String> visited = new HashSet<>(fieldProcessors.size());
            while (parser.hasToken(JsonToken.FIELD_NAME)) {
                final String fieldName = parser.currentName();
                final ValueProcessor knownProcessor = fieldProcessors.get(fieldName);
                if (knownProcessor == null) {
                    if (!options.allowUnknownFields()) {
                        // todo json exception
                        throw new IllegalStateException(String.format("Unexpected field '%s'", fieldName));
                    }
                    parser.nextToken();
                    parser.skipChildren();
                } else if (visited.add(fieldName)) {
                    // First time seeing field
                    parser.nextToken();
                    knownProcessor.processCurrentValue(parser);
                } else if (options.repeatedFieldBehavior() == RepeatedFieldBehavior.USE_FIRST) {
                    parser.nextToken();
                    parser.skipChildren();
                } else {
                    throw new IllegalStateException("todo");
                }
                parser.nextToken();
            }
            assertCurrentToken(parser, JsonToken.END_OBJECT);
            for (Entry<String, ValueProcessor> e : fieldProcessors.entrySet()) {
                if (!visited.contains(e.getKey())) {
                    e.getValue().processMissing(parser);
                }
            }
        }
    }

    final class ObjectValueRepeaterProcessor implements RepeaterProcessor {
        private final Map<String, RepeaterProcessor> fieldProcessors;


        public ObjectValueRepeaterProcessor(Map<String, RepeaterProcessor> fieldProcessors) {
            this.fieldProcessors = Objects.requireNonNull(fieldProcessors);
        }

        @Override
        public Context start(JsonParser parser) throws IOException {
            final Map<String, Context> contexts = new LinkedHashMap<>(fieldProcessors.size());
            for (Entry<String, RepeaterProcessor> e : fieldProcessors.entrySet()) {
                contexts.put(e.getKey(), e.getValue().start(parser));
            }
            return new ObjectArrayContext(contexts);
        }

        @Override
        public void processNullRepeater(JsonParser parser) throws IOException {
            for (RepeaterProcessor p : fieldProcessors.values()) {
                p.processNullRepeater(parser);
            }
        }

        @Override
        public void processMissingRepeater(JsonParser parser) throws IOException {
            for (RepeaterProcessor p : fieldProcessors.values()) {
                p.processMissingRepeater(parser);
            }
        }

        final class ObjectArrayContext implements Context {
            private final Map<String, Context> contexts;

            public ObjectArrayContext(Map<String, Context> contexts) {
                this.contexts = Objects.requireNonNull(contexts);
            }

            @Override
            public void processElement(JsonParser parser, int index) throws IOException {
                // see
                // com.fasterxml.jackson.databind.JsonDeserializer.deserialize(com.fasterxml.jackson.core.JsonParser,
                // com.fasterxml.jackson.databind.DeserializationContext)
                // for notes on FIELD_NAME
                switch (parser.currentToken()) {
                    case START_OBJECT:
                        if (parser.nextToken() == JsonToken.END_OBJECT) {
                            processEmptyObject(parser, index);
                            return;
                        }
                        if (!parser.hasToken(JsonToken.FIELD_NAME)) {
                            throw new IllegalStateException();
                        }
                        // fall-through
                    case FIELD_NAME:
                        processObjectFields(parser, index);
                        return;
                    case VALUE_NULL:
                        processNullObject(parser, index);
                        return;
                    default:
                        throw Parsing.mismatch(parser, Object.class);
                }
            }

            private void processNullObject(JsonParser parser, int ix) throws IOException {
                // element is null
                // pass-through JsonToken.VALUE_NULL
                for (Context context : contexts.values()) {
                    context.processElement(parser, ix);
                }
            }

            private void processEmptyObject(JsonParser parser, int ix) throws IOException {
                // This logic should be equivalent to processObjectFields, but where we know there are no fields
                for (Context context : contexts.values()) {
                    context.processElementMissing(parser, ix);
                }
            }

            private void processObjectFields(JsonParser parser, int ix) throws IOException {
                final Set<String> visited = new HashSet<>(contexts.size());
                while (parser.hasToken(JsonToken.FIELD_NAME)) {
                    final String fieldName = parser.currentName();
                    final Context knownProcessor = contexts.get(fieldName);
                    if (knownProcessor == null) {
                        if (!options.allowUnknownFields()) {
                            // todo json exception
                            throw new IllegalStateException(String.format("Unexpected field '%s'", fieldName));
                        }
                        parser.nextToken();
                        parser.skipChildren();
                    } else if (visited.add(fieldName)) {
                        // First time seeing field
                        parser.nextToken();
                        knownProcessor.processElement(parser, ix);
                    } else if (options.repeatedFieldBehavior() == RepeatedFieldBehavior.USE_FIRST) {
                        parser.nextToken();
                        parser.skipChildren();
                    } else {
                        throw new IllegalStateException("todo");
                    }
                    parser.nextToken();
                }
                assertCurrentToken(parser, JsonToken.END_OBJECT);
                for (Entry<String, Context> e : contexts.entrySet()) {
                    if (!visited.contains(e.getKey())) {
                        e.getValue().processElementMissing(parser, ix);
                    }
                }
            }

            @Override
            public void processElementMissing(JsonParser parser, int index) throws IOException {
                for (Context context : contexts.values()) {
                    context.processElementMissing(parser, index);
                }
            }

            @Override
            public void done(JsonParser parser, int length) throws IOException {
                for (Context context : contexts.values()) {
                    context.done(parser, length);
                }
            }
        }
    }
}
