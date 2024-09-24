//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.ID;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class ParquetFieldMap {

    public static ParquetFieldMap of(GroupType schema) {
        final Map<String, Type> fieldNameToType = new HashMap<>(schema.getFieldCount());
        final Map<ID, Type> fieldIdToType = new HashMap<>(schema.getFieldCount());
        for (Type field : schema.getFields()) {
            fieldNameToType.put(field.getName(), field);
            fieldIdToType.put(field.getId(), field);
        }
        return new ParquetFieldMap(fieldNameToType, fieldIdToType);
    }

    private final Map<String, Type> fieldNameToType;
    private final Map<ID, Type> fieldIdToType;

    ParquetFieldMap(Map<String, Type> fieldNameToType, Map<ID, Type> fieldIdToType) {
        this.fieldNameToType = Objects.requireNonNull(fieldNameToType);
        this.fieldIdToType = Objects.requireNonNull(fieldIdToType);
    }

    public Optional<Type> lookupFieldName(String fieldName) {
        return Optional.ofNullable(fieldNameToType.get(fieldName));
    }

    public Optional<Type> lookupFieldId(ID fieldId) {
        return Optional.ofNullable(fieldIdToType.get(fieldId));
    }
}
