/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.json.IntArrayValueProcessor.ElementProcessor;

import java.io.IOException;

class IntArrayValueProcessor extends ArrayValueProcessor<ElementProcessor> {
    private static final int[] EMPTY_INT_ARRAY = new int[0];

    private final WritableObjectChunk<int[], ?> chunk;
    private final int[] onNull;
    private final int[] onMissing;
    private final boolean allowNullElements;
    private final int onNullElement;

    IntArrayValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, WritableObjectChunk<int[], ?> chunk, int[] onNull, int[] onMissing, boolean allowNullElements, int onNullElement) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = chunk;
        this.onNull = onNull;
        this.onMissing = onMissing;
        this.allowNullElements = allowNullElements;
        this.onNullElement = onNullElement;
    }

    @Override
    protected ElementProcessor start() {
        return new ElementProcessor();
    }

    @Override
    protected void end(ElementProcessor valueProcessor) {
        chunk.add(valueProcessor.toArray());
    }

    @Override
    protected void handleNull() {
        chunk.add(onNull);
    }

    @Override
    protected void handleMissing() {
        chunk.add(onMissing);
    }

    class ElementProcessor extends ValueProcessorBase {
        private int[] array;
        private int len;

        public ElementProcessor() {
            // missing doesn't make sense in context of array
            super("element." + IntArrayValueProcessor.this.contextPrefix, allowNullElements, false);
            this.array = EMPTY_INT_ARRAY;
            this.len = 0;
        }

        public int[] toArray() {
            if (array.length == len) {
                return array;
            }
            final int[] properSize = new int[len];
            System.arraycopy(array, 0, properSize, 0, len);
            return properSize;
        }

        @Override
        protected void handleValueNumberInt(JsonParser parser) throws IOException {
            array = ArrayUtil.put(array, len, parser.getIntValue());
            ++len;
        }

        @Override
        protected void handleNull() {
            array = ArrayUtil.put(array, len, onNullElement);
            ++len;
        }

        @Override
        protected void handleMissing() {
            throw new IllegalStateException();
        }
    }
}
