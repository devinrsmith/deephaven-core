package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.stream.blink.tf.BooleanFunction;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;
import org.immutables.value.Value.Parameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.stream.blink.tf.BooleanFunction.map;
import static io.deephaven.stream.blink.tf.BooleanFunction.or;

/**
 * The {@link #path()} to a {@link com.google.protobuf.Descriptors.Descriptor Descriptor's} field.
 *
 * <p>
 * {@link FieldDescriptor} objects are not equal across {@link com.google.protobuf.DynamicMessage dynamic messages}.
 * Helpers {@link #numberPath()} and {@link #namePath()} may be used in lieu of direct equivalence depending on context.
 */
@Immutable
@SimpleStyle
public abstract class FieldPath {

    public static FieldPath empty() {
        return of(List.of());
    }

    public static FieldPath of(FieldDescriptor... descriptors) {
        return of(Arrays.asList(descriptors));
    }

    public static FieldPath of(List<FieldDescriptor> descriptors) {
        return ImmutableFieldPath.of(descriptors);
    }

    public static BooleanFunction<FieldPath> numberPathStartsWith(FieldNumberPath prefix) {
        return map(FieldPath::numberPath, x -> x.startsWith(prefix));
    }

    public static BooleanFunction<FieldPath> numberPathStartsWithUs(FieldNumberPath other) {
        return map(FieldPath::numberPath, other::startsWith);
    }

    public static BooleanFunction<FieldPath> anyNumberPathStartsWithUs(Collection<FieldNumberPath> numberPaths) {
        return or(numberPaths.stream().map(FieldPath::numberPathStartsWithUs).collect(Collectors.toList()));
    }

    public static BooleanFunction<FieldPath> namePathStartsWith(List<String> prefix) {
        return fieldPath -> fieldPath.startsWith(prefix);
    }

    public static BooleanFunction<FieldPath> anyNamePathStartsWith(Collection<List<String>> namePaths) {
        return or(namePaths.stream().map(FieldPath::namePathStartsWith).collect(Collectors.toList()));
    }

    public static BooleanFunction<FieldPath> namePathEquals(List<String> namePath) {
        return map(FieldPath::namePath, namePath::equals);
    }

    public static BooleanFunction<FieldPath> numberPathEquals(FieldNumberPath numberPath) {
        return map(FieldPath::numberPath, numberPath::equals);
    }

    @Parameter
    public abstract List<FieldDescriptor> path();

    @Lazy
    public FieldNumberPath numberPath() {
        return FieldNumberPath.of(path().stream().mapToInt(FieldDescriptor::getNumber).toArray());
    }

    @Lazy
    public List<String> namePath() {
        return path().stream().map(FieldDescriptor::getName).collect(Collectors.toList());
    }

    public final FieldPath prefixWith(FieldDescriptor prefix) {
        return FieldPath.of(Stream.concat(Stream.of(prefix), path().stream()).collect(Collectors.toList()));
    }

    public final FieldPath append(FieldDescriptor fieldDescriptor) {
        return FieldPath.of(Stream.concat(path().stream(), Stream.of(fieldDescriptor)).collect(Collectors.toList()));
    }

    public final boolean startsWith(List<String> prefix) {
        final List<String> us = namePath();
        return us.subList(0, Math.min(prefix.size(), us.size())).equals(prefix);
    }
}
