package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.stream.blink.tf.BooleanFunction;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * The {@link ProtobufDescriptorParser} options.
 *
 * @see ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)
 */
@Immutable
@BuildableStyle
public abstract class ProtobufDescriptorParserOptions {

    public static Builder builder() {
        return ImmutableProtobufDescriptorParserOptions.builder();
    }

    public static ProtobufDescriptorParserOptions defaults() {
        return builder().build();
    }

    /**
     * Controls which fields paths are included. By default, is {@link BooleanFunction#ofTrue()}.
     *
     * @return the fields to include function
     *
     */
    @Default
    public BooleanFunction<FieldPath> include() {
        return BooleanFunction.ofTrue();
    }

    /**
     * Controls which single-valued message parsers to use. By default, is {@link SingleValuedMessageParser#defaults()}.
     * 
     * @return the single-valued message parsers
     */
    @Default
    public List<SingleValuedMessageParser> parsers() {
        return SingleValuedMessageParser.defaults();
    }

    @Default
    public BooleanFunction<FieldPath> parseAsWellKnown() {
        return BooleanFunction.ofTrue();
    }

    @Default
    public BooleanFunction<FieldPath> parseAsBytes() {
        return BooleanFunction.ofTrue();
    }

    @Default
    public BooleanFunction<FieldPath> parseAsMap() {
        return BooleanFunction.ofTrue();
    }

    public interface Builder {
        Builder include(BooleanFunction<FieldPath> f);

        Builder parsers(List<SingleValuedMessageParser> parsers);

        Builder parseAsWellKnown(BooleanFunction<FieldPath> f);

        Builder parseAsBytes(BooleanFunction<FieldPath> f);

        Builder parseAsMap(BooleanFunction<FieldPath> f);

        ProtobufDescriptorParserOptions build();
    }
}
