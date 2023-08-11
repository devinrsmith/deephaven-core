package io.deephaven.protobuf;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
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

    public abstract Set<FieldNumberPath> includeNumberPaths();

    public abstract Set<List<String>> includeNamePaths();

    public abstract Set<FieldNumberPath> excludeNumberPaths();

    public abstract Set<List<String>> excludeNamePaths();

    @Default
    public List<SingleValuedMessageParser> parsers() {
        return SingleValuedMessageParser.defaults();
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

    final boolean include(FieldPath fieldPath) {
        if (hasIncludeNumberPathsStartsWith(fieldPath) || hasIncludeNamePathsStartsWith(fieldPath)) {
            return true;
        }
        // empty checks save us from materializing numberPath / namePath unnecessarily
        if (!includeNumberPaths().isEmpty() && includeNumberPaths().contains(fieldPath.numberPath())) {
            return true;
        }
        if (hasExcludeNumberPathsStartsWith(fieldPath) || hasExcludeNamePathsStartsWith(fieldPath)) {
            return false;
        }
        return true;
    }

    final boolean hasIncludeNumberPathsStartsWith(FieldPath other) {
        if (includeNumberPaths().isEmpty()) {
            return false;
        }
        for (FieldNumberPath numberPath : includeNumberPaths()) {
            if (other.startsWithUs(numberPath)) {
                return true;
            }
        }
        return false;
    }

    final boolean hasIncludeNamePathsStartsWith(FieldPath other) {
        if (includeNamePaths().isEmpty()) {
            return false;
        }
        for (List<String> namePath : includeNamePaths()) {
            if (other.startsWithUs(namePath)) {
                return true;
            }
        }
        return false;
    }

    final boolean hasExcludeNumberPathsStartsWith(FieldPath other) {
        if (excludeNumberPaths().isEmpty()) {
            return false;
        }
        for (FieldNumberPath numberPath : excludeNumberPaths()) {
            if (other.startsWithUs(numberPath)) {
                return true;
            }
        }
        return false;
    }

    final boolean hasExcludeNamePathsStartsWith(FieldPath other) {
        if (excludeNamePaths().isEmpty()) {
            return false;
        }
        for (List<String> namePath : excludeNamePaths()) {
            if (other.startsWithUs(namePath)) {
                return true;
            }
        }
        return false;
    }
}
