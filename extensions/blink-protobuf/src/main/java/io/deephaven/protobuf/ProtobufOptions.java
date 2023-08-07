package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
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

    public abstract Optional<String> unknownFieldSetName();

    public abstract Optional<String> serializedSizeName();

    public abstract Optional<String> rawMessageName();

    public abstract Set<List<String>> excludePaths();

    @Default
    public Map<Descriptor, SingleValuedMessageParser> parsers() {
        return SingleValuedMessageParser.defaults();
    }

    final boolean include(List<String> path) {
        return !excludePaths().contains(path);
    }

    final boolean parseAdHocMessage(FieldDescriptor fd, ProtobufOptions options, List<String> context) {
        return false;
        // return parseAdHocMessage(); // todo
    }

    final boolean parseAdHocRepeatedMessage(FieldDescriptor fd, ProtobufOptions options, List<String> context) {
        return false;
        // return parseAdHocMessage(); // todo
    }

    final Optional<String> unknownFieldSetName(Descriptor descriptor, ProtobufOptions options, List<String> context) {
        return unknownFieldSetName(); // todo
    }

    final Optional<String> serializedSizeName(Descriptor descriptor, ProtobufOptions options, List<String> context) {
        return serializedSizeName(); // todo
    }

    final Optional<String> rawMessageName(Descriptor descriptor, ProtobufOptions options, List<String> context) {
        return rawMessageName(); // todo
    }

    public interface Builder {

        Builder serializedSizeName(String serializedSizeName);

        Builder unknownFieldSetName(String unknownFieldSetName);

        Builder rawMessageName(String rawMessageName);

        Builder addExcludePaths(List<String> element);

        Builder addExcludePaths(List<String>... elements);

        Builder addAllExcludePaths(Iterable<? extends List<String>> elements);

        Builder parsers(Map<Descriptor, SingleValuedMessageParser> parsers);

        ProtobufOptions build();
    }
}
