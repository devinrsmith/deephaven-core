/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

import static io.deephaven.processor.ObjectProcessor.strict;
import static org.assertj.core.api.Assertions.assertThat;

public class ObjectProcessorMapTest {

    static class MyObject {
        private final int foo;
        private final long bar;

        public MyObject(int foo, long bar) {
            this.foo = foo;
            this.bar = bar;
        }
    }

    enum MyObjectProcessor implements ObjectProcessor<MyObject> {
        INSTANCE;

        @Override
        public List<Type<?>> outputTypes() {
            return List.of(Type.intType(), Type.longType());
        }

        @Override
        public void processAll(ObjectChunk<? extends MyObject, ?> in, List<WritableChunk<?>> out) {
            final WritableIntChunk<?> foo = out.get(0).asWritableIntChunk();
            final WritableLongChunk<?> bar = out.get(1).asWritableLongChunk();
            for (int i = 0; i < in.size(); i++) {
                final MyObject myObject = in.get(i);
                foo.add(myObject.foo);
                bar.add(myObject.bar);
            }
        }
    }

    static class NestedMyObject {
        private final MyObject nested;

        public NestedMyObject(MyObject nested) {
            this.nested = Objects.requireNonNull(nested);
        }

        public MyObject nested() {
            return nested;
        }
    }

    static class DoubleNestedMyObject {
        private final NestedMyObject nested;

        public DoubleNestedMyObject(NestedMyObject nested) {
            this.nested = Objects.requireNonNull(nested);
        }

        public NestedMyObject nested() {
            return nested;
        }
    }

    @Test
    public void testMap() {
        final ObjectProcessor<NestedMyObject> mapped =
                strict(ObjectProcessor.map(NestedMyObject::nested, strict(MyObjectProcessor.INSTANCE)));
        assertThat(mapped.outputTypes()).containsExactly(Type.intType(), Type.longType());
        try (
                WritableObjectChunk<NestedMyObject, ?> in = WritableObjectChunk.makeWritableChunk(1);
                WritableIntChunk<?> c1 = WritableIntChunk.makeWritableChunk(1);
                WritableLongChunk<?> c2 = WritableLongChunk.makeWritableChunk(1)) {
            List<WritableChunk<?>> out = List.of(c1, c2);
            for (WritableChunk<?> c : out) {
                c.setSize(0);
            }
            in.set(0, new NestedMyObject(new MyObject(42, 43L)));
            c1.set(0, 0);
            c2.set(0, 0L);
            mapped.processAll(in, out);
            for (WritableChunk<?> c : out) {
                assertThat(c.size()).isEqualTo(1);
            }
            assertThat(c1.get(0)).isEqualTo(42);
            assertThat(c2.get(0)).isEqualTo(43L);
        }
    }

    @Test
    public void testDoubleMap() {
        final ObjectProcessor<DoubleNestedMyObject> mapped = strict(ObjectProcessor.map(DoubleNestedMyObject::nested,
                ObjectProcessor.map(NestedMyObject::nested, strict(MyObjectProcessor.INSTANCE))));
        assertThat(mapped.outputTypes()).containsExactly(Type.intType(), Type.longType());
        try (
                WritableObjectChunk<DoubleNestedMyObject, ?> in = WritableObjectChunk.makeWritableChunk(1);
                WritableIntChunk<?> c1 = WritableIntChunk.makeWritableChunk(1);
                WritableLongChunk<?> c2 = WritableLongChunk.makeWritableChunk(1)) {
            List<WritableChunk<?>> out = List.of(c1, c2);
            for (WritableChunk<?> c : out) {
                c.setSize(0);
            }
            in.set(0, new DoubleNestedMyObject(new NestedMyObject(new MyObject(42, 43L))));
            c1.set(0, 0);
            c2.set(0, 0L);
            mapped.processAll(in, out);
            for (WritableChunk<?> c : out) {
                assertThat(c.size()).isEqualTo(1);
            }
            assertThat(c1.get(0)).isEqualTo(42);
            assertThat(c2.get(0)).isEqualTo(43L);
        }
    }
}
