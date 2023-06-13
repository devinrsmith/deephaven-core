/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.util;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import io.deephaven.blink.Protobuf;
import io.deephaven.blink.ProtobufBlink;
import io.deephaven.blink.ProtobufOptions;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.UncoalescedTable;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse.What;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.stream.blink.BlinkTableMapper;
import io.deephaven.util.SafeCloseable;

import java.util.List;

public class ExportUtil {

    public static BlinkTableMapper<ExportedTableCreationResponse> MAPPER;
    private static LivenessScope scope;

    public static BlinkTableMapper<ExportedTableCreationResponse> mapper() {
        synchronized (ExportUtil.class) {
            if (MAPPER == null) {
                scope = new LivenessScope();
                try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
                    MAPPER = ProtobufBlink.create(ExportedTableCreationResponse.getDescriptor(),
                            ProtobufOptions.builder()
                                    .unknownFieldSetName("unknown_field_set")
                                    .serializedSizeName("serialized_size")
                                    .rawMessageName("raw_message")
                                    .addExcludePaths(List.of("result_id", "batch_offset"), List.of("schema_header"))
                                    .build());
                }
            }
            return MAPPER;
        }
    }

    public static Table table() {
        return mapper().table();
    }

    public static ExportedTableCreationResponse buildTableCreationResponse(Ticket ticket, Table table) {
        return buildTableCreationResponse(TableReference.newBuilder().setTicket(ticket).build(), table);
    }

    public static ExportedTableCreationResponse buildTableCreationResponse(TableReference tableRef, Table table) {
        final long size;
        if (table instanceof UncoalescedTable) {
            size = Long.MIN_VALUE;
        } else {
            size = table.size();
        }
        final ExportedTableCreationResponse etcr = ExportedTableCreationResponse.newBuilder()
                .setSuccess(true)
                .setResultId(tableRef)
                .setIsStatic(!table.isRefreshing())
                .setSize(size)
                .setSchemaHeader(BarrageUtil.schemaBytesFromTable(table))
                .putMyMap("mykey", "myvalue")
                .putMyMap2("some", What.DOWN)
                .putMyMap2("ok", What.UP)
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(9999, Field.newBuilder().addFixed32(32).build())
                        .build())
                .build();

        try {
            final DynamicMessage etcr2 =
                    DynamicMessage.parseFrom(ExportedTableCreationResponse.getDescriptor(), etcr.toByteArray());
            int t = 0;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        mapper().producer().add(etcr);
        return etcr;
    }
}
