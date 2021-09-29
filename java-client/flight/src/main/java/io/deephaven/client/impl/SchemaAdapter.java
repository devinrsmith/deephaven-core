package io.deephaven.client.impl;

import io.deephaven.qst.table.TableHeader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;

public class SchemaAdapter {
    public static Schema of(TableHeader header) {
        final FieldType fieldType = new FieldType(true, MinorType.INT.getType(), null, Collections.singletonMap("deephaven:type", "int"));
        return new Schema(Collections.singleton(new Field("X", fieldType, null)));
    }
}
