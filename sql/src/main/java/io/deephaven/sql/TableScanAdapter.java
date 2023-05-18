/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.sql;

import io.deephaven.qst.table.TableSpec;
import org.apache.calcite.rel.core.TableScan;

final class TableScanAdapter {

    public static TableSpec namedTable(TableScan table) {
        // TODO: use fully qualified name
        return TableSpec.ticket("scan/" + table.getTable().getQualifiedName().get(0));
    }
}
