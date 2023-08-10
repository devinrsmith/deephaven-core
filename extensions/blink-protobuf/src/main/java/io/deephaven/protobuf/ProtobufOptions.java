package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.protobuf.ImmutableProtobufOptions.Builder;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Immutable
@BuildableStyle
public abstract class ProtobufOptions {

    public static Builder builder() {
        return ImmutableProtobufOptions.builder();
    }

    public static ProtobufOptions defaults() {
        return builder().build();
    }

    public abstract Optional<String> unknownFieldSetName();

    public abstract Optional<String> serializedSizeName();

    public abstract Optional<String> rawMessageName();

    public abstract Set<FieldNumberPath> includeNumberPaths();

    public abstract Set<List<String>> includeNamePaths();

    public abstract Set<FieldNumberPath> excludeNumberPaths();

    public abstract Set<List<String>> excludeNamePaths();

    @Default
    public List<SingleValuedMessageParser> parsers() {
        return SingleValuedMessageParser.defaults();
    }

    final boolean include(FieldPath fieldPath) {
        // empty checks save us from materializing numberPath / namePath unnecessarily
        if (!includeNumberPaths().isEmpty() && includeNumberPaths().contains(fieldPath.numberPath())) {
            return true;
        }
        if (!includeNamePaths().isEmpty() && includeNamePaths().contains(fieldPath.namePath())) {
            return true;
        }
        if (!excludeNumberPaths().isEmpty() && excludeNumberPaths().contains(fieldPath.numberPath())) {
            return false;
        }
        if (!excludeNamePaths().isEmpty() && excludeNamePaths().contains(fieldPath.namePath())) {
            return false;
        }
        return true;
    }

    @Check
    final void checkConditions() {
        final boolean hasIncludes = !includeNamePaths().isEmpty() || !includeNumberPaths().isEmpty();
        final boolean hasExcludes = !excludeNamePaths().isEmpty() || !excludeNumberPaths().isEmpty();
        if (hasIncludes && hasExcludes) {
            throw new IllegalArgumentException("ProtobufOptions may only have includes or excludes, not both");
        }
    }

    public interface Builder {

        Builder serializedSizeName(String serializedSizeName);

        Builder unknownFieldSetName(String unknownFieldSetName);

        Builder rawMessageName(String rawMessageName);

        Builder addIncludeNumberPaths(FieldNumberPath element);

        Builder addIncludeNumberPaths(FieldNumberPath... elements);

        Builder addAllIncludeNumberPaths(Iterable<? extends FieldNumberPath> elements);

        Builder addIncludeNamePaths(List<String> element);

        Builder addIncludeNamePaths(List<String>... elements);

        Builder addAllIncludeNamePaths(Iterable<? extends List<String>> elements);

        Builder addExcludeNumberPaths(FieldNumberPath element);

        Builder addExcludeNumberPaths(FieldNumberPath... elements);

        Builder addAllExcludeNumberPaths(Iterable<? extends FieldNumberPath> elements);

        Builder addExcludeNamePaths(List<String> element);

        Builder addExcludeNamePaths(List<String>... elements);

        Builder addAllExcludeNamePaths(Iterable<? extends List<String>> elements);

        Builder parsers(List<SingleValuedMessageParser> parsers);

        ProtobufOptions build();
    }
}
