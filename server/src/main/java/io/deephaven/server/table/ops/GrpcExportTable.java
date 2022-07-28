package io.deephaven.server.table.ops;

import io.deephaven.annotations.LeafStyle;
import io.deephaven.qst.table.ImplementationTable;
import io.deephaven.qst.table.TableSpec;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.Collections;
import java.util.List;

@Immutable
@LeafStyle
abstract class GrpcExportTable extends ImplementationTable {

    public static GrpcExportTable of(int index) {
        return ImmutableGrpcExportTable.of(index);
    }

    @Value.Parameter
    public abstract int index();

    @Override
    public final List<TableSpec> parents() {
        return Collections.emptyList();
    }

    @Value.Check
    final void checkIndex() {
        if (index() < 0) {
            throw new IllegalArgumentException("index must be non-negative");
        }
    }
}
