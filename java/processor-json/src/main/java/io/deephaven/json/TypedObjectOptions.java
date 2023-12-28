/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A type-discriminated object.
 */
@Immutable
@BuildableStyle
public abstract class TypedObjectOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableTypedObjectOptions.builder();
    }

    public abstract String typeFieldName();

    // todo: shared fields?

    public abstract Map<String, ObjectOptions> types();

    @Default
    public boolean allowUnknownTypes() {
        return true;
    }

    public interface Builder extends ValueOptions.Builder<TypedObjectOptions, Builder> {
        Builder typeFieldName(String typeFieldName);

        Builder putTypes(String key, ObjectOptions value);

        Builder putTypes(Map.Entry<String, ? extends ObjectOptions> entry);

        Builder putAllTypes(Map<String, ? extends ObjectOptions> entries);

        Builder allowUnknownTypes(boolean allowUnknownTypes);
    }

    @Check
    final void checkProcessorsSupportMissing() {
        if (!types().values().stream().allMatch(ObjectOptions::allowMissing)) {
            throw new IllegalArgumentException("Must support missing todo...");
        }
    }

    @Override
    Stream<Type<?>> outputTypes() {
        return Stream.concat(
                Stream.of(Type.stringType()),
                types().values().stream().flatMap(ObjectOptions::outputTypes));
    }

    @Override
    ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        final Map<String, Processor> processors = new LinkedHashMap<>(types().size());
        // Note: ix = 1; ix = 0 is for typeOut
        int ix = 1;
        for (Entry<String, ObjectOptions> e : types().entrySet()) {
            final String type = e.getKey();
            final ObjectOptions opts = e.getValue();
            final int numTypes = opts.numColumns();
            final List<WritableChunk<?>> chunksOut = out.subList(ix, ix + numTypes);
            final ValueProcessor processor = opts.processor(context + "[" + type + "]", chunksOut);
            processors.put(type, new Processor(processor, chunksOut));
            ix += numTypes;
        }
        if (ix != out.size()) {
            throw new IllegalStateException();
        }
        return new DescriminatedProcessor(out.get(0).asWritableObjectChunk(), processors);
    }

    private String parseTypeField(JsonParser parser) throws IOException {
        final String currentFieldName = parser.currentName();
        if (!typeFieldName().equals(currentFieldName)) {
            throw new IOException("todo");
        }
        switch (parser.nextToken()) {
            case VALUE_STRING:
                return parser.getText();
            case VALUE_NULL:
                // todo: allowNullType?
                if (!allowNull()) {
                    throw Helpers.mismatch(parser, String.class);
                }
                // todo: onNull value?
                return null;
            default:
                throw Helpers.mismatch(parser, String.class);
        }
    }

    private static class Processor {
        private final ValueProcessor valueProcessor;
        private final List<WritableChunk<?>> out;

        public Processor(ValueProcessor valueProcessor, List<WritableChunk<?>> out) {
            this.valueProcessor = valueProcessor;
            this.out = out;
        }

        ValueProcessor processor() {
            return valueProcessor;
        }

        void notApplicable() {
            for (WritableChunk<?> wc : out) {
                final int size = wc.size();
                wc.fillWithNullValue(size, 1);
                wc.setSize(size + 1);
            }
        }
    }

    class DescriminatedProcessor implements ValueProcessor {

        private final WritableObjectChunk<String, ?> typeOut;
        private final Map<String, Processor> processors;

        public DescriminatedProcessor(WritableObjectChunk<String, ?> typeOut, Map<String, Processor> processors) {
            this.typeOut = Objects.requireNonNull(typeOut);
            this.processors = Objects.requireNonNull(processors);
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_OBJECT:
                    if (parser.nextToken() == JsonToken.END_OBJECT) {
                        processEmptyObject(parser);
                        return;
                    }
                    if (parser.currentToken() != JsonToken.FIELD_NAME) {
                        throw new IllegalStateException();
                    }
                    // fall-through
                case FIELD_NAME:
                    processObjectFields(parser);
                    break;
                case VALUE_NULL:
                    processNullObject(parser);
                    break;
                default:
                    throw Helpers.mismatch(parser, Object.class); // todo
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Helpers.mismatchMissing(parser, Object.class); // todo
            }
            // onMissingType()?
            typeOut.add(null);
            for (Processor processor : processors.values()) {
                processor.notApplicable();
            }
        }

        private void processNullObject(JsonParser parser) throws IOException {
            if (!allowNull()) {
                throw Helpers.mismatch(parser, Object.class); // todo
            }
            // onNullType()?
            typeOut.add(null);
            for (Processor processor : processors.values()) {
                processor.notApplicable();
            }
        }

        private void processEmptyObject(JsonParser parser) throws IOException {
            // this logic should be equivalent to processObjectFields w/ no fields
            // suggests that maybe this branch should be an error b/c we _need_ type field?
            throw new IOException("no field");
        }

        private void processObjectFields(JsonParser parser) throws IOException {
            final String typeFieldValue = parseTypeField(parser);
            if (!allowUnknownTypes()) {
                if (!processors.containsKey(typeFieldValue)) {
                    throw new IOException("todo - unmapped type");
                }
            }
            typeOut.add(typeFieldValue);
            if (parser.nextToken() == JsonToken.END_OBJECT) {
                for (Processor processor : processors.values()) {
                    processor.notApplicable();
                }
            }
            if (parser.currentToken() != JsonToken.FIELD_NAME) {
                throw new IllegalStateException();
            }
            for (Entry<String, Processor> e : processors.entrySet()) {
                final String processorType = e.getKey();
                final Processor processor = e.getValue();
                if (processorType.equals(typeFieldValue)) {
                    processor.processor().processCurrentValue(parser);
                } else {
                    processor.notApplicable();
                }
            }
        }
    }
}
