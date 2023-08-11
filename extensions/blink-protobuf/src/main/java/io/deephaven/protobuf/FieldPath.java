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

    public static BooleanFunction<FieldPath> numberPathStartsWith(FieldNumberPath numberPath) {
        return map(FieldPath::numberPath, numberPath::startsWith);
    }

    public static BooleanFunction<FieldPath> anyNumberPathStartsWith(Collection<FieldNumberPath> numberPaths) {
        return or(numberPaths.stream().map(FieldPath::numberPathStartsWith).collect(Collectors.toList()));
    }

    public static BooleanFunction<FieldPath> namePathStartsWith(List<String> namePath) {
        return fieldPath -> fieldPath.startsWithUs(namePath);
    }

    public static BooleanFunction<FieldPath> anyNamePathStartsWith(Collection<List<String>> namePaths) {
        return or(namePaths.stream().map(FieldPath::namePathStartsWith).collect(Collectors.toList()));
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

    public final boolean startsWithUs(List<String> other) {
        // this is an "inversion" of startsWith; but helps us b/c we can't extend List
        final List<String> us = namePath();
        return other.subList(0, Math.min(other.size(), us.size())).equals(us);
    }
}
