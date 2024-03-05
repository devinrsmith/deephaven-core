/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

final class ArrayProcessorObjectImpl<T> extends ArrayProcessorBase<T[]> {
    private final ToObject<T> toObject;
    private final Class<T> clazz;

    public ArrayProcessorObjectImpl(Consumer<? super T[]> consumer, boolean allowMissing, boolean allowNull,
            T[] onMissing, T[] onNull, ToObject<T> toObject, Class<T> clazz) {
        super(consumer, allowMissing, allowNull, onMissing, onNull);
        this.toObject = Objects.requireNonNull(toObject);
        this.clazz = Objects.requireNonNull(clazz);
    }

    @Override
    public ArrayProcessorBase<T[]>.ArrayContextBase newContext() {
        return new ArrayContextObjectImpl();
    }

    final class ArrayContextObjectImpl extends ArrayContextBase {
        @SuppressWarnings("unchecked")
        private T[] arr = (T[]) Array.newInstance(clazz, 0);
        private int len;

        @Override
        public void processElement(int ix, JsonParser parser) throws IOException {
            arr = ArrayUtil.put(arr, ix, toObject.parseValue(parser), clazz);
            ++len;
        }

        @Override
        public void processElementMissing(int ix, JsonParser parser) throws IOException {
            arr = ArrayUtil.put(arr, ix, toObject.parseMissing(parser), clazz);
            ++len;
        }

        @Override
        public T[] onDone() {
            return len == arr.length ? arr : Arrays.copyOf(arr, len);
        }
    }
}
