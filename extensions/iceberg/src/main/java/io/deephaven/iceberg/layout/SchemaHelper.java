package io.deephaven.iceberg.layout;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class SchemaHelper {

//    public static Stream<NestedField> fieldPath(Schema schema, Iterator<String> iterator) {
//        return StreamSupport.stream(
//                Spliterators.spliteratorUnknownSize(new ItByName(schema, iterator), Spliterator.ORDERED | Spliterator.NONNULL),
//                false);
//    }

//    public static boolean contains(Schema schema, int... idPath) {
//        if (idPath.length == 0) {
//            return true;
//        }
//        final NestedField firstField = schema.findField(idPath[0]);
//        if (firstField == null) {
//            return false;
//        }
//        return contains(firstField, idPath, 1, idPath.length - 1);
//    }
//
//    private static boolean contains(NestedField field, int[] idPath, int offset, int len) {
//        if (len == 0) {
//            return true;
//        }
//        if (!field.type().isNestedType()) {
//            throw new IllegalArgumentException("todo");
//        }
//        final NestedField next = field.type().asNestedType().field(idPath[offset]);
//        if (next == null) {
//            return false;
//        }
//        return contains(next, idPath, offset + 1, len - 1);
//    }

    public static List<NestedField> nestedFields(Schema schema, int... idPath) {
        final List<NestedField> out = new ArrayList<>(idPath.length);
        if (idPath.length == 0) {
            return out;
        }
        NestedField current = schema.findField(idPath[0]);
        if (current == null) {
            throw idPathNotFound(idPath, out);
        }
        out.add(current);
        for (int i = 1; i < idPath.length; ++i) {
            if (!current.type().isNestedType()) {
                throw idPathTooLong(idPath, out);
            }
            current = current.type().asNestedType().field(idPath[i]);
            if (current == null) {
                throw idPathNotFound(idPath, out);
            }
            out.add(current);
        }
        return out;
    }


//    public static Stream<NestedField> nestedFields(Schema schema, PrimitiveIterator.OfInt idPath) {
//        if (!idPath.hasNext()) {
//            return Stream.empty();
//        }
//        final int firstId = idPath.nextInt();
//        final NestedField firstField = schema.findField(firstId);
//        if (firstField == null) {
//            throw invalidIdPath(firstId, 0);
//        }
//        return Stream.concat(
//                Stream.of(firstField),
//                StreamSupport.stream(
//                        Spliterators.spliteratorUnknownSize(new ItById(idPath, firstField), Spliterator.ORDERED | Spliterator.NONNULL), false));
//    }

//    public static Stream<Type> nestedFields(Schema schema, Iterator<String> namePath) {
//        if (!namePath.hasNext()) {
//            return Stream.empty();
//        }
//        final String firstName = namePath.next();
//        final Type firstField = schema.findType(firstName);
//        if (firstField == null) {
//            throw invalidNamePath(firstName, 0);
//        }
//        schema.findField()
//
//
//
//        return Stream.concat(
//                Stream.of(firstField.type()),
//                StreamSupport.stream(
//                        Spliterators.spliteratorUnknownSize(new ItByName(namePath, fi), Spliterator.ORDERED | Spliterator.NONNULL), false));
//    }
//


//    public static void walk(NestedField field, Consumer<NestedField> consumer) {
//        if (field.type().isNestedType()) {
//            for (NestedField nestedField : field.type().asNestedType().fields()) {
//                consumer.accept(nestedField);
//            }
//        }
//    }
//
//
//    private static class ItById extends AbstractIterator<NestedField> {
//        private final PrimitiveIterator.OfInt it;
//        private NestedField computed;
//        private int computedIx;
//
//        public ItById(PrimitiveIterator.OfInt it, NestedField computed) {
//            this.it = Objects.requireNonNull(it);
//            this.computed = Objects.requireNonNull(computed);
//            this.computedIx = 0;
//        }
//
//        @Override
//        protected @Nullable NestedField computeNext() {
//            if (computed == null || !it.hasNext()) {
//                computed = null;
//                return endOfData();
//            }
//            if (!computed.type().isNestedType()) {
//                throw new IllegalArgumentException("Invalid path, is too long");
//            }
//            final int fieldId = it.nextInt();
//            computed = computed.type().asNestedType().field(fieldId);
//            ++computedIx;
//            if (computed == null) {
//                throw invalidIdPath(fieldId, computedIx);
//            }
//            return computed;
//        }
//    }

    // TODO: special handling for "key", "value", "element"
//    private static class ItByName extends AbstractIterator<Type> {
//        private final Iterator<String> it;
//        private Type computed;
//        private int computedIx;
//
//        public ItByName(Iterator<String> it, Type computed) {
//            this.it = Objects.requireNonNull(it);
//            this.computed = Objects.requireNonNull(computed);
//            this.computedIx = 0;
//        }
//
//        @Override
//        protected @Nullable Type computeNext() {
//            if (computed == null || !it.hasNext()) {
//                computed = null;
//                return endOfData();
//            }
//            if (!computed.isNestedType()) {
//                throw new IllegalArgumentException("Must be nested, illegal path");
//            }
//            final String name = it.next();
//            computed = computed.asNestedType().fieldType(name);
//            ++computedIx;
//            if (computed == null) {
//                throw invalidNamePath(name, computedIx);
//            }
//            return computed;
//        }
//    }

    private static IllegalArgumentException idPathNotFound(int[] id, List<NestedField> context) {
        final int ix = context.size();
        final String contextStr = context.stream().map(NestedField::name).collect(Collectors.joining("', '", "['", "']"));
        throw new IllegalArgumentException(String.format("id path not found, path=%s, context=%s", Arrays.toString(id), contextStr));
    }

    private static IllegalArgumentException idPathTooLong(int[] id, List<NestedField> context) {
        final int ix = context.size();
        final String contextStr = context.stream().map(NestedField::name).collect(Collectors.joining("', '", "['", "']"));
        throw new IllegalArgumentException(String.format("id path too long, path=%s, context=%s", Arrays.toString(id), contextStr));
    }

//    private static IllegalArgumentException invalidNamePath(String name, int ix) {
//        throw new IllegalArgumentException(String.format("Invalid name path, name=%s @ ix=%d", name, ix));
//    }
}
