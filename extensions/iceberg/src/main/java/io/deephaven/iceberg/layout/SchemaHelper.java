package io.deephaven.iceberg.layout;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class SchemaHelper {

    public static Stream<NestedField> fieldPath(Schema schema, Iterator<String> iterator) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(new ItByName(schema, iterator), Spliterator.ORDERED | Spliterator.NONNULL),
                false);
    }

    public static Stream<NestedField> fieldPath(Schema schema, PrimitiveIterator.OfInt iterator) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(new ItById(schema, iterator), Spliterator.ORDERED | Spliterator.NONNULL),
                false);
    }

    private static class ItByName implements Iterator<NestedField> {
        private final Iterator<String> it;
        private NestedField next;

        ItByName(Schema schema, Iterator<String> it) {
            this.it = Objects.requireNonNull(it);
            this.next = Objects.requireNonNull(schema.findField(it.next()));
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public NestedField next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            try {
                return next;
            } finally {
                if (!it.hasNext()) {
                    next = null;
                } else {
                    next = Objects.requireNonNull(next.type().asStructType().field(it.next()));
                }
            }
        }
    }

    private static class ItById implements Iterator<NestedField> {
        private final PrimitiveIterator.OfInt it;
        private NestedField next;

        ItById(Schema schema, PrimitiveIterator.OfInt it) {
            this.it = Objects.requireNonNull(it);
            this.next = Objects.requireNonNull(schema.findField(it.nextInt()));
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public NestedField next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            try {
                return next;
            } finally {
                if (!it.hasNext()) {
                    next = null;
                } else {
                    next = Objects.requireNonNull(next.type().asStructType().field(it.nextInt()));
                }
            }
        }
    }
}
