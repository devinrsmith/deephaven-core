/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.json.IntArrayChunkValueProcessor.ElementProcessor;
import io.deephaven.util.QueryConstants;

import java.io.IOException;

class IntArrayChunkValueProcessor extends ArrayObjectChunkValueProcessorBase<int[], ElementProcessor> {
    private static final int[] EMPTY = new int[0];
    private final boolean allowNullElements;

    public IntArrayChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableObjectChunk<int[], ?> chunk, boolean allowNullElements) {
        super(contextPrefix, allowNull, allowMissing, chunk);
        this.allowNullElements = allowNullElements;
    }

    @Override
    protected ElementProcessor start() {
        return new ElementProcessor();
    }

    @Override
    protected void end(ElementProcessor valueProcessor) {
        chunk.add(valueProcessor.toArray());
    }

    class ElementProcessor extends ValueProcessorBase {
        private int[] array;
        private int len;

        public ElementProcessor() {
            // missing doesn't make sense in context of array
            super(IntArrayChunkValueProcessor.this.context + "[.]", allowNullElements, false);
            this.array = EMPTY;
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

        private void put(int value) {
            array = ArrayUtil.put(array, len, value);
            ++len;
        }

        @Override
        protected void handleValueNumberInt(JsonParser parser) throws IOException {
            put(parser.getIntValue());
        }

        @Override
        protected void handleNull(JsonParser parser) throws IOException {
            put(QueryConstants.NULL_INT);
        }

        @Override
        protected void handleMissing(JsonParser parser) throws IOException {
            throw new IllegalStateException();
        }
    }
}
