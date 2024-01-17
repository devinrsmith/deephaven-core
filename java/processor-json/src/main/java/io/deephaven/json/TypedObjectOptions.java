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
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.util.ArrayList;
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

    public abstract Map<String, ValueOptions> sharedFields();

    public abstract Map<String, ObjectOptions> objects();

    @Default
    public boolean allowUnknownTypes() {
        return true;
    }

    public interface Builder extends ValueOptions.Builder<TypedObjectOptions, Builder> {
        // python needs this to be overloaded... :/
        @Override
        Builder allowNull(boolean allowNull);

        @Override
        Builder allowMissing(boolean allowMissing);

        Builder typeFieldName(String typeFieldName);

        Builder putSharedFields(String key, ValueOptions value);

        Builder putSharedFields(Map.Entry<String, ? extends ValueOptions> entry);

        Builder putAllSharedFields(Map<String, ? extends ValueOptions> entries);

        Builder putObjects(String key, ObjectOptions value);

        Builder putObjects(Map.Entry<String, ? extends ObjectOptions> entry);

        Builder putAllObjects(Map<String, ? extends ObjectOptions> entries);

        Builder allowUnknownTypes(boolean allowUnknownTypes);
    }

    @Override
    Stream<Type<?>> outputTypes() {
        return Stream.concat(
                Stream.of(Type.stringType()),
                Stream.concat(
                        sharedFields().values().stream().flatMap(ValueOptions::outputTypes),
                        objects().values().stream().flatMap(ObjectOptions::outputTypes)));
    }

    @Override
    ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        final WritableObjectChunk<String, ?> typeOut = out.get(0).asWritableObjectChunk();
        final List<WritableChunk<?>> sharedFields = out.subList(1, 1 + sharedFields().size());
        final Map<String, Processor> processors = new LinkedHashMap<>(objects().size());
        int outIx = 1 + sharedFields.size();
        for (Entry<String, ObjectOptions> e : objects().entrySet()) {
            final String type = e.getKey();
            final ObjectOptions specificOpts = e.getValue();
            final int numSpecificFields = specificOpts.numColumns();
            final List<WritableChunk<?>> specificChunks = out.subList(outIx, outIx + numSpecificFields);
            final List<WritableChunk<?>> allChunks = concat(sharedFields, specificChunks);
            final ObjectOptions combinedObject = combinedObject(specificOpts);
            final ValueProcessor processor = combinedObject.processor(context + "[" + type + "]", allChunks);
            processors.put(type, new Processor(processor, specificChunks));
            outIx += numSpecificFields;
        }
        if (outIx != out.size()) {
            throw new IllegalStateException();
        }
        return new DescriminatedProcessor(typeOut, processors);
    }

    private static <T> List<T> concat(List<T> x, List<T> y) {
        if (x.isEmpty()) {
            return y;
        }
        if (y.isEmpty()) {
            return x;
        }
        final List<T> out = new ArrayList<>(x.size() + y.size());
        out.addAll(x);
        out.addAll(y);
        return out;
    }

    private ObjectOptions combinedObject(ObjectOptions objectOpts) {
        final Map<String, ValueOptions> sharedFields = sharedFields();
        if (sharedFields.isEmpty()) {
            return objectOpts;
        }
        final ObjectOptions.Builder builder = ObjectOptions.builder()
                .allowUnknownFields(objectOpts.allowUnknownFields())
                .allowNull(objectOpts.allowNull())
                .allowMissing(objectOpts.allowMissing());
        builder.putAllFields(sharedFields);
        builder.putAllFields(objectOpts.fields());
        return builder.build();
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
        private final List<WritableChunk<?>> specificChunks;

        public Processor(ValueProcessor valueProcessor, List<WritableChunk<?>> specificChunks) {
            this.valueProcessor = Objects.requireNonNull(valueProcessor);
            this.specificChunks = Objects.requireNonNull(specificChunks);
        }

        ValueProcessor processor() {
            return valueProcessor;
        }

        void notApplicable() {
            // We should be able to set this to false depending on the context - users should use the type to
            // discriminate.
            boolean nullOutput = true;
            for (WritableChunk<?> wc : specificChunks) {
                final int size = wc.size();
                if (nullOutput) {
                    wc.fillWithNullValue(size, 1);
                }
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
