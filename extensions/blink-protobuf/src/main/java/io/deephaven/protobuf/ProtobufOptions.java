package io.deephaven.protobuf;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.stream.blink.tf.BooleanFunction;
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

    @Default
    public BooleanFunction<FieldPath> include() {
        return BooleanFunction.ofTrue();
    }

    @Default
    public List<SingleValuedMessageParser> parsers() {
        return SingleValuedMessageParser.defaults();
    }

    public interface Builder {
        Builder include(BooleanFunction<FieldPath> f);

        Builder parsers(List<SingleValuedMessageParser> parsers);

        ProtobufOptions build();
    }

    final boolean include(FieldPath fieldPath) {
        return include().applyAsBoolean(fieldPath);
    }
}
