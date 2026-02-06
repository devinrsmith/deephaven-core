package io.deephaven.kafka.avro;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

@Value.Immutable
@BuildableStyle
public abstract class Options {
    // final String schemaName,
    //                final String schemaVersion,
    //                final Function<String, String> fieldPathToColumnName,
    //                final boolean useUTF8String

    public abstract SchemaProvider schemaProvider();

    @Value.Default
    public Protocol protocol() {
        return Protocol.serdes();
    }

    @Value.Default
    public boolean useUTF8String() {
        return false;
    }

    // Function<String, String> fieldPathToColumnName
}
