//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.format;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
public abstract class Schema {

    static Schema of(MessageType schema) {
        return null;
    }

    @Value.Parameter
    abstract MessageType schema();

    public final List<Field> fields() {

        for (Type field : schema().getFields()) {

        }
    }
}
