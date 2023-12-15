/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.json.GenericArrayChunkValueProcessor.ElementProcessor2;

class GenericArrayChunkValueProcessor<T> extends ArrayObjectChunkValueProcessorBase<T[], ElementProcessor2> {
    private final boolean allowNullElements;

    public GenericArrayChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableObjectChunk<T[], ?> chunk, boolean allowNullElements) {
        super(contextPrefix, allowNull, allowMissing, chunk);
        this.allowNullElements = allowNullElements;
    }

    @Override
    protected ElementProcessor2 start() {
        return null;
    }

    @Override
    protected void end(ElementProcessor2 valueProcessor) {

    }

    // @Override
    // protected ElementProcessor start() {
    // return new ElementProcessor();
    // }
    //
    // @Override
    // protected void end(ElementProcessor valueProcessor) {
    // chunk.add(valueProcessor.toArray());
    // }

    static class ElementProcessor2 extends ValueProcessorBase {
        public ElementProcessor2(String context, boolean allowNull, boolean allowMissing) {
            super(context, allowNull, allowMissing);
        }

        @Override
        protected void handleNull() {

        }

        @Override
        protected void handleMissing() {

        }
    }
}
