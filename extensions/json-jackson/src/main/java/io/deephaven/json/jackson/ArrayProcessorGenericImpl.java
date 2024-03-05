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

final class ArrayProcessorGenericImpl<T> extends ArrayProcessorBase<T[]> {
    private final ToObject<T> toObject;
    private final Class<T> componentClazz;
    private final Class<? extends T[]> arrayClazz;

    public ArrayProcessorGenericImpl(Consumer<? super T[]> consumer, boolean allowMissing, boolean allowNull,
            T[] onMissing, T[] onNull, ToObject<T> toObject, Class<T> componentClazz, Class<? extends T[]> arrayClazz) {
        super(consumer, allowMissing, allowNull, onMissing, onNull);
        this.toObject = Objects.requireNonNull(toObject);
        this.componentClazz = Objects.requireNonNull(componentClazz);
        this.arrayClazz = Objects.requireNonNull(arrayClazz);
    }

    @Override
    public ArrayContextGenericImpl newContext() {
        return new ArrayContextGenericImpl();
    }

    final class ArrayContextGenericImpl extends ArrayContextBase {
        @SuppressWarnings("unchecked")
        private T[] arr = (T[]) Array.newInstance(componentClazz, 0);
        private int len;

        @Override
        public void processElement(JsonParser parser, int index) throws IOException {
            arr = ArrayUtil.put(arr, index, toObject.parseValue(parser), componentClazz);
            ++len;
        }

        @Override
        public void processElementMissing(JsonParser parser, int index) throws IOException {
            arr = ArrayUtil.put(arr, index, toObject.parseMissing(parser), componentClazz);
            ++len;
        }

        @Override
        public T[] onDone(int length) {
            if (length != len) {
                throw new IllegalStateException();
            }
            return len == arr.length ? arr : Arrays.copyOf(arr, len, arrayClazz);
        }
    }
}
