//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ParquetUtilTest {

    // Default equality / comparison does not compare maxDef / maxRep
    private static final Comparator<ColumnDescriptor> COLUMN_DESCRIPTOR_COMPARATOR = Comparator
            .<ColumnDescriptor>naturalOrder()
            .thenComparingInt(ColumnDescriptor::getMaxDefinitionLevel)
            .thenComparingInt(ColumnDescriptor::getMaxRepetitionLevel);

    @Test
    public void getColumnsEmpty() {
        final MessageType schema = Types.buildMessage().named("root");
        final List<ColumnDescriptor> columns = ParquetUtil.getColumns(schema);
        assertThat(columns)
                .usingElementComparator(COLUMN_DESCRIPTOR_COMPARATOR)
                .isEqualTo(schema.getColumns());
    }

    @Test
    public void getColumns() {
        final PrimitiveType required = Types.required(PrimitiveTypeName.INT32).named("Required");
        final PrimitiveType repeated = Types.repeated(PrimitiveTypeName.INT32).named("Repeated");
        final PrimitiveType optional = Types.optional(PrimitiveTypeName.INT32).named("Optional");
        final GroupType requiredGroup = Types.requiredGroup()
                .addFields(required, repeated, optional)
                .named("RequiredGroup");
        final GroupType repeatedGroup = Types.repeatedGroup()
                .addFields(required, repeated, optional)
                .named("RepeatedGroup");
        final GroupType optionalGroup = Types.optionalGroup()
                .addFields(required, repeated, optional)
                .named("OptionalGroup");
        final GroupType requiredGroup2 = Types.requiredGroup()
                .addFields(required, repeated, optional, requiredGroup, repeatedGroup, optionalGroup)
                .named("RequiredGroup2");
        final GroupType repeatedGroup2 = Types.repeatedGroup()
                .addFields(required, repeated, optional, requiredGroup, repeatedGroup, optionalGroup)
                .named("RepeatedGroup2");
        final GroupType optionalGroup2 = Types.optionalGroup()
                .addFields(required, repeated, optional, requiredGroup, repeatedGroup, optionalGroup)
                .named("OptionalGroup2");
        final MessageType schema = Types.buildMessage()
                .addFields(required, repeated, optional, requiredGroup, repeatedGroup, optionalGroup, requiredGroup2,
                        repeatedGroup2, optionalGroup2)
                .named("root");
        assertThat(ParquetUtil.getColumns(schema))
                .usingElementComparator(COLUMN_DESCRIPTOR_COMPARATOR)
                .isEqualTo(schema.getColumns());
    }
}
