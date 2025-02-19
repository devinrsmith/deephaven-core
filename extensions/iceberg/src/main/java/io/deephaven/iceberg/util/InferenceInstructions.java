package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.iceberg.internal.NameMappingUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
@BuildableStyle
public abstract class InferenceInstructions {

    public abstract Table table();

    @Value.Default
    public Schema schema() {
        return table().schema();
    }

    @Value.Default
    public PartitionSpec spec() {
        return table().spec();
    }

    @Value.Default
    public boolean useNameMapping() {
        return true;
    }

    @Value.Default
    public boolean failOnUnsupportedTypes() {
        return false;
    }

    final Optional<NameMapping> nameMapping() {
        return useNameMapping()
                ? NameMappingUtil.readNameMappingDefault(table())
                : Optional.empty();
    }

    @Value.Check
    final void checkSpecSchema() {
        if (spec() == PartitionSpec.unpartitioned()) {
            return;
        }
        if (!schema().sameSchema(spec().schema())) {
            throw new IllegalArgumentException("schema and spec schema are not the same");
        }
    }
}
