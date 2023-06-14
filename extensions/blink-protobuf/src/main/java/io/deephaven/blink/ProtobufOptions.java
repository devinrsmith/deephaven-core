package io.deephaven.blink;

import io.deephaven.annotations.BuildableStyle;
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

    // todo: ensure this works for Timestamp and others (Any doesn't count)
    public abstract Optional<String> unknownFieldSetName();

    public abstract Optional<String> serializedSizeName();

    public abstract Optional<String> rawMessageName();

    public abstract Set<List<String>> excludePaths();

    @Default
    public Map<String, MessageTypeParser> parsers() {
        return MessageTypeParser.defaults();
    }

    final boolean include(List<String> path) {
        return !excludePaths().contains(path);
    }

    public interface Builder {

        Builder serializedSizeName(String serializedSizeName);

        Builder unknownFieldSetName(String unknownFieldSetName);

        Builder rawMessageName(String rawMessageName);

        Builder addExcludePaths(List<String> element);

        Builder addExcludePaths(List<String>... elements);

        Builder addAllExcludePaths(Iterable<? extends List<String>> elements);

        Builder parsers(Map<String, MessageTypeParser> parsers);

        ProtobufOptions build();
    }
}