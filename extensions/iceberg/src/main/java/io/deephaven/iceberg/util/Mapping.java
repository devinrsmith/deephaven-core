package io.deephaven.iceberg.util;

import io.deephaven.iceberg.layout.IcebergMapping;
import org.apache.iceberg.Schema;

interface Mapping {

    static Mapping infer() {
        // infer at runtime, before user has schema; etc
        return null;
    }

    static Mapping infer(Schema schema) {
        // infer from this; same logic as runtime inference would use, but this is upfront
        return null;
    }

    static Mapping updateInfer(IcebergMapping mapping, Schema schema) {
        return null;
    }


    IcebergMapping getMapping(Schema runtime);

    // inferNew
}
